import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

import java.io.File;
import java.nio.file.Paths;
import java.util.Objects;

public class FileProcessor {
    static class ExtractIdAndFilenameFromLine extends DoFn<KV<String, String>, String> {
        private final Counter linesProcessed = Metrics.counter(FileProcessor.class, "linesProcessed");

        @ProcessElement
        public void processElement(@Element KV<String, String> element, OutputReceiver<String> out) {
            String filename = element.getKey();
            String line = element.getValue();
            
            try {
                // Split the line by pipe delimiter
                String[] fields = line.split("\\|");
                if (fields.length > 0) {
                    String id = fields[0].trim();
                    // Get absolute path
                    String absolutePath = new File(filename).getAbsolutePath();
                    // Output in format: ID,AbsoluteFilePath
                    out.output(id + "," + absolutePath);
                    linesProcessed.inc();
                }
            } catch (Exception e) {
                System.err.println("Error processing line from file " + filename + ": " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        // Set parallelism for local execution
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(PipelineOptions.class);
        options.setRunner(org.apache.beam.runners.direct.DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        // Replace this with your input folder path
        String inputFolderPath = "/path/to/your/input/folder/*.txt";

        PCollection<String> results = pipeline
            // Read all text files in the directory
            .apply("ReadFiles", FileIO.match().filepattern(inputFolderPath))
            .apply("ReadMatches", FileIO.readMatches())
            // Read file contents and include filename
            .apply("ReadContents", ParDo.of(new DoFn<FileIO.ReadableFile, KV<String, String>>() {
                @ProcessElement
                public void processElement(@Element FileIO.ReadableFile file,
                                        OutputReceiver<KV<String, String>> out) {
                    try {
                        String filename = file.getMetadata().resourceId().toString();
                        String contents = file.readFullyAsUTF8String();
                        for (String line : contents.split("\n")) {
                            if (!line.trim().isEmpty()) {
                                out.output(KV.of(filename, line));
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error reading file: " + e.getMessage());
                    }
                }
            }))
            // Extract ID and filename
            .apply("ExtractIdAndFilename", ParDo.of(new ExtractIdAndFilenameFromLine()));

        // Write results to output file
        results.apply("WriteResults", TextIO.write()
            .to("id_file_mapping")
            .withSuffix(".csv")
            .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
