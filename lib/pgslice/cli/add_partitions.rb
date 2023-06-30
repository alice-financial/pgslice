module PgSlice
  class CLI
    desc "add_partitions TABLE", "Add partitions"
    option :intermediate, type: :boolean, default: false, desc: "Add to intermediate table"
    option :past, type: :numeric, default: 0, desc: "Number of past partitions to add"
    option :future, type: :numeric, default: 0, desc: "Number of future partitions to add"
    option :tablespace, type: :string, default: "", desc: "Tablespace to use"
    option :use_view, type: :boolean, default: false, desc: "use a view"
    def add_partitions(table)
      original_table = create_table(table)
      table = options[:intermediate] ? original_table.intermediate_table : original_table
      trigger_name = original_table.trigger_name

      assert_table(table)

      future = options[:future]
      past = options[:past]
      tablespace = options[:tablespace]
      range = (-1 * past)..future

      period, field, cast, needs_comment, declarative, version = table.fetch_settings(original_table.trigger_name)
      unless period
        message = "No settings found: #{table}"
        message = "#{message}\nDid you mean to use --intermediate?" unless options[:intermediate]
        abort message
      end

      queries = []

      if needs_comment
        queries << "COMMENT ON TRIGGER #{quote_ident(trigger_name)} ON #{quote_table(table)} IS 'column:#{field},period:#{period},cast:#{cast}';"
      end

      # today = utc date
      today = round_date(Time.now.utc.to_date, period)

      schema_table =
        if !declarative
          table
        elsif options[:intermediate]
          original_table
        else
          table.partitions.last
        end

      # indexes automatically propagate in Postgres 11+
      if version < 3
        index_defs = schema_table.index_defs
        fk_defs = schema_table.foreign_keys
      else
        index_defs = []
        fk_defs = []
      end

      primary_key = schema_table.primary_key
      tablespace_str = tablespace.empty? ? "" : " TABLESPACE #{quote_ident(tablespace)}"

      added_partitions = []
      range.each do |n|
        day = advance_date(today, period, n)

        partition = Table.new(original_table.schema, "#{original_table.name}_#{day.strftime(name_format(period))}")
        next if partition.exists?
        added_partitions << partition

        if declarative
          queries << <<-SQL
CREATE TABLE #{quote_table(partition)} PARTITION OF #{quote_table(table)} FOR VALUES FROM (#{sql_date(day, cast, false)}) TO (#{sql_date(advance_date(day, period, 1), cast, false)})#{tablespace_str};
          SQL
        else
          queries << <<-SQL
CREATE TABLE #{quote_table(partition)}
    (CHECK (#{quote_ident(field)} >= #{sql_date(day, cast)} AND #{quote_ident(field)} < #{sql_date(advance_date(day, period, 1), cast)}))
    INHERITS (#{quote_table(table)})#{tablespace_str};
          SQL
        end

        queries << "ALTER TABLE #{quote_table(partition)} ADD PRIMARY KEY (#{primary_key.map { |k| quote_ident(k) }.join(", ")});" if primary_key.any?

        index_defs.each do |index_def|
          queries << make_index_def(index_def, partition)
        end

        fk_defs.each do |fk_def|
          queries << make_fk_def(fk_def, partition)
        end
      end

      unless declarative
        # update trigger based on existing partitions
        current_defs = []
        future_defs = []
        past_defs = []
        name_format = self.name_format(period)
        partitions = (table.partitions + added_partitions).uniq(&:name).sort_by(&:name)

        partitions.each do |partition|
          day = partition_date(partition, name_format)

          # note: does not support generated columns
          # could support by listing columns
          # but this would cause issues with schema changes
          sql = "(NEW.#{quote_ident(field)} >= #{sql_date(day, cast)} AND NEW.#{quote_ident(field)} < #{sql_date(advance_date(day, period, 1), cast)}) THEN
              INSERT INTO #{quote_table(partition)} VALUES (NEW.*);"

          if day.to_date < today
            past_defs << sql
          elsif advance_date(day, period, 1) < today
            current_defs << sql
          else
            future_defs << sql
          end
        end

        # order by current period, future periods asc, past periods desc
        trigger_defs = current_defs + future_defs + past_defs.reverse

        if trigger_defs.any?
          queries << <<-SQL
CREATE OR REPLACE FUNCTION #{quote_ident(trigger_name)}()
    RETURNS trigger AS $$
    BEGIN
        IF #{trigger_defs.join("\n        ELSIF ")}
        ELSE
            RAISE EXCEPTION 'Date out of range. Ensure partitions are created.';
        END IF;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
          SQL
        end
      end

      # If you try to do this more than once on an intermediate table, it will fail. Not sure if it is normal/expected
      # for someone to do this. Could be addressed by splitting out the `use_view` functionality, or by just
      # leaving that off if you need to add more partitions to an intermediate table.
      if options[:intermediate] && options[:use_view]
        retired_table = original_table.retired_table
        primary_key = original_table.primary_key.first
        sequence = original_table.sequences.detect{|x| x['related_column'] == primary_key }
        sequence_name = sequence ? sequence['sequence_name'] : nil
        trig_query = <<-SQL
CREATE OR REPLACE FUNCTION partitioned_view_trigger()
returns trigger
language plpgsql
as
$TRIG$

begin
    IF TG_OP = 'INSERT'
    THEN
        #{sequence_name ? "NEW.#{quote_ident(primary_key)} := nextval('#{table.schema}.\"#{sequence_name}\"');" : ""}
        INSERT INTO #{quote_table(table)} VALUES(NEW.*);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE'
    THEN
        DELETE FROM #{quote_table(table)} WHERE #{quote_ident(primary_key)} = OLD.#{quote_ident(primary_key)};
        DELETE FROM #{quote_table(retired_table)} WHERE #{quote_ident(primary_key)} = OLD.#{quote_ident(primary_key)};
        RETURN OLD;
    ELSE -- UPDATE
        DELETE FROM #{quote_table(retired_table)} WHERE #{quote_ident(primary_key)} = OLD.#{quote_ident(primary_key)};
        IF FOUND
        THEN
            INSERT INTO #{quote_table(table)} VALUES(NEW.*);
        ELSE
            UPDATE #{quote_table(table)} SET #{quote_ident(primary_key)} = NEW.#{quote_ident(primary_key)}, data = NEW.data
                WHERE #{quote_ident(primary_key)} = OLD.#{quote_ident(primary_key)};
        END IF;
        RETURN NEW;
    END IF;
end

$TRIG$;
        SQL
        run_queries([trig_query])

        queries << <<-SQL
    ALTER TABLE #{quote_no_schema(original_table)} RENAME TO #{quote_no_schema(retired_table)};
    SQL

        # No need for autovacuum on this table, since we won't be doing INSERTs or UPDATEs on it any more.
        queries << <<-SQL
    ALTER TABLE #{quote_table(retired_table)} SET(
      autovacuum_enabled = false, toast.autovacuum_enabled = false
   );
   SQL

        queries << <<-SQL
    CREATE VIEW #{quote_table(original_table)} AS
    SELECT * FROM #{quote_table(table)}
    UNION ALL
    SELECT * FROM #{quote_table(retired_table)}
    ;
    SQL

        queries << <<-SQL
    CREATE TRIGGER partition_trigger
    INSTEAD OF INSERT OR UPDATE OR DELETE ON #{quote_table(original_table)}
    FOR EACH ROW
    EXECUTE FUNCTION partitioned_view_trigger();
        SQL
      end

      run_queries(queries) if queries.any?
    end
  end
end
