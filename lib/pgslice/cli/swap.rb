module PgSlice
  class CLI
    desc "swap TABLE", "Swap the intermediate table with the original table"
    option :lock_timeout, default: "5s", desc: "Lock timeout"
    option :use_view, type: :boolean, default: false, desc: "Use view with trigger for partiioning"
    def swap(table)
      table = create_table(table)
      intermediate_table = table.intermediate_table
      retired_table = table.retired_table

      if options[:use_view]
        assert_view(table)
      else
        assert_table(table)
        assert_no_table(retired_table)
      end
      assert_table(intermediate_table)

      if options[:use_view]
        queries = [
          "DROP VIEW #{quote_table(table)} CASCADE;",
          "DROP FUNCTION partitioned_view_trigger();",
          "ALTER TABLE #{quote_table(intermediate_table)} RENAME TO #{quote_no_schema(table)};"
        ]
        retired_table.sequences.each do |sequence|
          queries << "ALTER SEQUENCE #{quote_ident(sequence["sequence_schema"])}.#{quote_ident(sequence["sequence_name"])} OWNED BY #{quote_table(table)}.#{quote_ident(sequence["related_column"])};"
        end
      else
        queries = [
          "ALTER TABLE #{quote_table(table)} RENAME TO #{quote_no_schema(retired_table)};",
          "ALTER TABLE #{quote_table(intermediate_table)} RENAME TO #{quote_no_schema(table)};"
        ]
        table.sequences.each do |sequence|
          queries << "ALTER SEQUENCE #{quote_ident(sequence["sequence_schema"])}.#{quote_ident(sequence["sequence_name"])} OWNED BY #{quote_table(table)}.#{quote_ident(sequence["related_column"])};"
        end
      end


      queries.unshift("SET LOCAL lock_timeout = #{escape_literal(options[:lock_timeout])};")

      run_queries(queries)
    end
  end
end
