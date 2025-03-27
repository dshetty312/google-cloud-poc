import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class OptimizedDynamicAvroSchemaBigQueryJob {

    // Singleton helper to generate schema only once
    public static class SchemaGenerator implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final AtomicReference<Schema> SCHEMA_CACHE = new AtomicReference<>(null);

        // Synchronized method to generate schema only once
        public static Schema generateSchema() {
            // Double-checked locking pattern for thread-safety
            if (SCHEMA_CACHE.get() == null) {
                synchronized (SchemaGenerator.class) {
                    if (SCHEMA_CACHE.get() == null) {
                        try {
                            Schema generatedSchema = createDynamicSchema();
                            SCHEMA_CACHE.set(generatedSchema);
                        } catch (SQLException e) {
                            throw new RuntimeException("Failed to generate schema", e);
                        }
                    }
                }
            }
            return SCHEMA_CACHE.get();
        }

        // Actual schema generation logic
        private static Schema createDynamicSchema() throws SQLException {
            try (Connection connection = DatabaseConnectionUtil.getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                
                // Assume we're working with a specific table
                String tableName = "your_table_name";
                String schemaName = "your_schema_name";

                // Build Avro schema dynamically
                List<Schema.Field> fields = new ArrayList<>();

                try (ResultSet columns = metaData.getColumns(null, schemaName, tableName, null)) {
                    while (columns.next()) {
                        String columnName = columns.getString("COLUMN_NAME");
                        int sqlType = columns.getInt("DATA_TYPE");
                        
                        // Map SQL types to Avro types
                        Schema fieldSchema = mapSqlTypeToAvroSchema(sqlType);
                        
                        // Create Avro field
                        fields.add(new Schema.Field(columnName, 
                            fieldSchema, 
                            null, 
                            null));
                    }
                }

                // Create the full Avro schema
                return Schema.createRecord("DynamicRecord", null, null, false, fields);
            }
        }

        // Helper method to map SQL types to Avro schemas
        private static Schema mapSqlTypeToAvroSchema(int sqlType) {
            switch (sqlType) {
                case java.sql.Types.INTEGER:
                    return Schema.create(Schema.Type.INT);
                case java.sql.Types.BIGINT:
                    return Schema.create(Schema.Type.LONG);
                case java.sql.Types.FLOAT:
                    return Schema.create(Schema.Type.FLOAT);
                case java.sql.Types.DOUBLE:
                    return Schema.create(Schema.Type.DOUBLE);
                case java.sql.Types.VARCHAR:
                case java.sql.Types.CHAR:
                case java.sql.Types.NVARCHAR:
                    return Schema.create(Schema.Type.STRING);
                case java.sql.Types.DATE:
                    return Schema.create(Schema.Type.STRING); // Or use logical type
                default:
                    return Schema.create(Schema.Type.STRING); // Fallback
            }
        }
    }

    // DoFn to convert input line to Avro GenericRecord
    public static class LineToAvroRecordFn extends DoFn<String, GenericRecord> {
        private transient Schema schema;

        @Setup
        public void setup() {
            // Retrieve the pre-generated schema
            schema = SchemaGenerator.generateSchema();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element();

            // Parse the line based on your file layout (example uses CSV)
            String[] fields = line.split(",");

            // Create GenericRecord
            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);

            // Populate record based on schema fields
            List<Schema.Field> schemaFields = schema.getFields();
            for (int i = 0; i < schemaFields.size() && i < fields.length; i++) {
                Schema.Field field = schemaFields.get(i);
                String fieldName = field.name();
                Schema.Type fieldType = field.schema().getType();

                // Convert and set field value based on type
                Object value = convertValue(fields[i], fieldType);
                recordBuilder.set(fieldName, value);
            }

            c.output(recordBuilder.build());
        }

        // Helper method to convert string to appropriate type
        private Object convertValue(String value, Schema.Type type) {
            if (value == null || value.isEmpty()) return null;
            
            switch (type) {
                case INT:
                    return Integer.parseInt(value);
                case LONG:
                    return Long.parseLong(value);
                case FLOAT:
                    return Float.parseFloat(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case STRING:
                    return value;
                default:
                    return value;
            }
        }
    }

    public static void main(String[] args) {
        // Create pipeline options
        PipelineOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .create();

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Pre-generate the schema before pipeline execution (optional optimization)
        Schema dynamicSchema = SchemaGenerator.generateSchema();

        // Read input file
        PCollection<String> inputLines = pipeline
            .apply("ReadInputFile", TextIO.read().from("gs://your-input-bucket/input-file.csv"));

        // Convert lines to Avro records
        PCollection<GenericRecord> avroRecords = inputLines
            .apply("ConvertToAvroRecords", ParDo.of(new LineToAvroRecordFn()));

        // Write to BigQuery
        avroRecords.apply("WriteToBigQuery", 
            BigQueryIO.writeGenericRecords()
                .to(createTableReference(options))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // Run the pipeline
        pipeline.run();
    }

    // Helper method to create BigQuery table reference
    private static TableReference createTableReference(PipelineOptions options) {
        TableReference tableRef = new TableReference();
        tableRef.setProjectId(options.as(DataflowPipelineOptions.class).getProject());
        tableRef.setDatasetId("your_dataset");
        tableRef.setTableId("your_table");
        return tableRef;
    }

    // Utility class for database connection (implement with your connection details)
    private static class DatabaseConnectionUtil {
        public static Connection getConnection() throws SQLException {
            // Implement your database connection logic
            // Example for MySQL:
            // return DriverManager.getConnection("jdbc:mysql://hostname:port/database", "username", "password");
            throw new UnsupportedOperationException("Implement database connection");
        }
    }
}
