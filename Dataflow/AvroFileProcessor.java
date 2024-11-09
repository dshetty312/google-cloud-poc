import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.avro.generic.GenericRecord;

public class AvroFileProcessor {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(PipelineOptions.class);
        options.setRunner(org.apache.beam.runners.direct.DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        // Replace with your input folder path
        String inputFolderPath = "/path/to/your/input/folder/part-*.avro";

        PCollection<String> results = pipeline
            // Match Avro files
            .apply("FindAvroFiles", FileIO.match().filepattern(inputFolderPath))
            .apply("ReadMatches", FileIO.readMatches())
            // Read Avro records with file metadata
            .apply("ReadAvroFiles", ParDo.of(new DoFn<FileIO.ReadableFile, KV<String, GenericRecord>>() {
                @ProcessElement
                public void processElement(@Element FileIO.ReadableFile file,
                                        OutputReceiver<KV<String, GenericRecord>> out) {
                    String filename = file.getMetadata().resourceId().toString();
                    
                    // Use AvroIO to read the file
                    Pipeline.create().apply(AvroIO.readGenericRecords(file.getMetadata().resourceId().toString())
                        .withBeamSchemas(true))
                        .apply(ParDo.of(new DoFn<GenericRecord, Void>() {
                            @ProcessElement
                            public void processElement(@Element GenericRecord record) {
                                out.output(KV.of(filename, record));
                            }
                        }));
                }
            }))
            // Extract CID and filename
            .apply("ExtractCidAndFilename", ParDo.of(new DoFn<KV<String, GenericRecord>, String>() {
                @ProcessElement
                public void processElement(@Element KV<String, GenericRecord> element,
                                        OutputReceiver<String> out) {
                    String filename = element.getKey();
                    GenericRecord record = element.getValue();
                    String cid = record.get("cid").toString();
                    String absolutePath = new File(filename).getAbsolutePath();
                    out.output(cid + "," + absolutePath);
                }
            }));

        // Write results
        results.apply("WriteResults", TextIO.write()
            .to("cid_file_mapping")
            .withSuffix(".csv")
            .withHeader("CID,FilePath")
            .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
