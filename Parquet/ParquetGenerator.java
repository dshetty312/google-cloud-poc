import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class ParquetGenerator {
    public static void main(String[] args) throws IOException {
        // Create a sample list of records
        List<GenericRecord> records = Arrays.asList(
            createRecord("John", 30, "New York"),
            createRecord("Jane", 25, "San Francisco"),
            createRecord("Bob", 35, "Chicago"),
            createRecord("Alice", 28, "Miami")
        );

        // Create the Parquet directory
        String parquetDir = "parquet_files";
        Files.createDirectories(Paths.get(parquetDir));

        // Write the records to Parquet files
        for (int i = 0; i < 3; i++) {
            writeParquetFile(records, parquetDir, "data_" + (i + 1) + ".parquet");
        }

        System.out.println("Sample Parquet files created in '" + parquetDir + "'");
    }

    private static GenericRecord createRecord(String name, int age, String city) {
        GenericRecord record = new GenericData.Record(
            org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Person\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"city\",\"type\":\"string\"}]}"));
        record.put("name", name);
        record.put("age", age);
        record.put("city", city);
        return record;
    }

    private static void writeParquetFile(List<GenericRecord> records, String directory, String fileName) throws IOException {
        Path path = new Path(directory, fileName);
        Configuration conf = new Configuration();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .withConf(conf)
             .build()) {
            for (GenericRecord record : records) {
                writer.write(record);
            }
        }
    }
}
