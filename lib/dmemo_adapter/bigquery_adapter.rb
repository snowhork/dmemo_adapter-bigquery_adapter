require 'google/cloud/bigquery'

module DmemoAdapter
  module BigqueryAdapter
    class Adapter < Base
      DmemoAdapter.register 'bigquery', self

      def initialize(data_source)
        @bq = Google::Cloud::Bigquery.new(project_id: 'bigquery-public-data')
        @bq_dataset = @bq.dataset('samples')
        @bq_tables = {}
      end

      def fetch_tables
        @bq_dataset.tables.each_with_object [] do |table, result|
          next if table.gapi.type == "MODEL"
          result << Table.new(@bq_dataset.dataset_id, table.table_id)
        end
      end

      def fetch_rows(table, limit)
        table = bq_table(table.table_name)

        table.data(max: limit).map do |row|
          extract_row_data(row, [], table.fields)
        end
      end

      def fetch_columns(table)
        flatten_fields(bq_table(table.table_name).fields, [], "")
      end

      def fetch_count(table)
        bq_table(table.table_name).rows_count
      end

      def reset!
        @bq = nil
        @bq_dataset = nil
        @bq_tables = nil
      end

      def disconnect!
      end

      private

      def bq_table(table_id)
        @bq_tables[table_id] || @bq_tables[table_id] = @bq_dataset.table(table_id)
      end

      def flatten_fields(fields, result, field_prefix)
        fields.each do |field|
          nullable = field.mode.nil? || field.mode == "NULLABLE"
          result << Column.new("#{field_prefix}#{field.name}", field.type, "", nullable)

          flatten_fields(field.fields, result, "#{field_prefix}#{field.name}.") if field.type == "RECORD"
        end
        result
      end

      def extract_row_data(row, result, fields)
        fields.each do |field|
          if field.type == "RECORD"
            result << ""
            extract_row_data(row, result, field.fields)
          else
            result << row[field.name.to_sym]
          end
        end
        result
      end
    end
  end
end
