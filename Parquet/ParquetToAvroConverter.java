import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class ParquetToAvroConverter {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        String inputDir = "parquet_files";
        String outputDir = "avro_files";

        PCollection<GenericRecord> records = pipeline
            .apply("Read Parquet Files", ParquetIO.read(GenericRecord.class).from(inputDir + "/*.parquet"));

        records.apply("Write Avro Files", AvroIO.write(GenericRecord.class)
            .to(outputDir)
            .withSuffix(".avro")
            .withCodec("deflate")
            .withNumShards(1));

        pipeline.run().waitUntilFinished();
    }
}
