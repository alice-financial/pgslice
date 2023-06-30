module PgSlice
  class CLI
    desc "fill TABLE", "Fill the partitions in batches"
    option :batch_size, type: :numeric, default: 10000, desc: "Batch size"
    option :swapped, type: :boolean, default: false, desc: "Use swapped table"
    option :source_table, desc: "Source table"
    option :dest_table, desc: "Destination table"
    option :start, type: :numeric, desc: "Primary key to start"
    option :where, desc: "Conditions to filter"
    option :sleep, type: :numeric, desc: "Seconds to sleep between batches"
    option :use_view, type: :boolean, desc: "use view"
    def fill(table)
      table = create_table(table)
      source_table = create_table(options[:source_table]) if options[:source_table]
      dest_table = create_table(options[:dest_table]) if options[:dest_table]

      if options[:swapped]
        source_table ||= table.retired_table
        dest_table ||= table
      elsif options[:use_view]
        source_table = table.retired_table
        dest_table ||= table
      else
        source_table ||= table
        dest_table ||= table.intermediate_table
      end

      if options[:use_view]
        assert_table(source_table)
        assert_view(dest_table)
      else
        assert_table(source_table)
        assert_table(dest_table)
      end

      period, field, cast, _, declarative, _ = dest_table.fetch_settings(table.trigger_name)

      if period
        name_format = self.name_format(period)

        if options[:use_view]
          partitions = table.intermediate_table.partitions
        else
          partitions = dest_table.partitions
        end
        if partitions.any?
          starting_time = partition_date(partitions.first, name_format)
          ending_time = advance_date(partition_date(partitions.last, name_format), period, 1)
        end
      end

      if options[:use_view]
        partitions = table.intermediate_table.partitions
        primary_key = partitions.last.primary_key
      else
        schema_table = period && declarative ? partitions.last : table
        primary_key = schema_table.primary_key[0]
      end

      abort "No primary key" unless primary_key

      max_source_id = nil
      begin
        max_source_id = source_table.max_id(primary_key)
      rescue PG::UndefinedFunction
        abort "Only numeric primary keys are supported"
      end

      max_dest_id =
        if options[:start]
          options[:start]
        elsif options[:swapped]
          dest_table.max_id(primary_key, where: options[:where], below: max_source_id)
        else
          dest_table.max_id(primary_key, where: options[:where])
        end

      if options[:use_view]
        query = <<-SQL
        WITH old_ids AS
  (
    SELECT #{quote_ident(primary_key)}
    FROM #{quote_table(source_table)}
    LIMIT 10000
  ),
  ROWS AS (
  UPDATE #{quote_table(dest_table)}
  SET #{quote_ident(primary_key)} = #{quote_ident(primary_key)}
  WHERE #{quote_ident(primary_key)} IN (SELECT #{quote_ident(primary_key)} FROM old_ids)
  RETURNING 1)
  SELECT COUNT(*) FROM ROWS;
        SQL

        loop do
          tups = run_query(query)
          rows_done = tups[0]["count"].to_i
          break if rows_done == 0 || !rows_done
        end
      else

        if max_dest_id == 0 && !options[:swapped]
          min_source_id = source_table.min_id(primary_key, field, cast, starting_time, options[:where])
          max_dest_id = min_source_id - 1 if min_source_id
        end

        starting_id = max_dest_id
        fields = source_table.columns.map { |c| quote_ident(c) }.join(", ")
        batch_size = options[:batch_size]

        i = 1
        batch_count = ((max_source_id - starting_id) / batch_size.to_f).ceil

        if batch_count == 0
          log_sql "/* nothing to fill */"
        end

        while starting_id < max_source_id
          where = "#{quote_ident(primary_key)} > #{starting_id} AND #{quote_ident(primary_key)} <= #{starting_id + batch_size}"
          if starting_time
            where << " AND #{quote_ident(field)} >= #{sql_date(starting_time, cast)} AND #{quote_ident(field)} < #{sql_date(ending_time, cast)}"
          end
          if options[:where]
            where << " AND #{options[:where]}"
          end

          query = <<-SQL
  /* #{i} of #{batch_count} */
  INSERT INTO #{quote_table(dest_table)} (#{fields})
      SELECT #{fields} FROM #{quote_table(source_table)}
      WHERE #{where}
          SQL

          run_query(query)

          starting_id += batch_size
          i += 1

          if options[:sleep] && starting_id <= max_source_id
            sleep(options[:sleep])
          end
        end
      end
    end
  end
end
