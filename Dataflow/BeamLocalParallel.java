import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Watch;
import java.util.Arrays;
import java.io.File;
import java.util.List;
import java.util.ArrayList;

public class BeamLocalParallel {
    static class FileInfo {
        String filename;
        String content;  // In real scenario, this could be your file content or path

        FileInfo(String filename, String content) {
            this.filename = filename;
            this.content = content;
        }
    }

    public static void main(String[] args) {
        // Set parallelism
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");
        
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(PipelineOptions.class);
        
        options.setRunner(org.apache.beam.runners.direct.DirectRunner.class);
        
        Pipeline pipeline = Pipeline.create(options);

        // Create sample file data
        List<FileInfo> files = Arrays.asList(
            new FileInfo("file1.txt", "content1"),
            new FileInfo("file2.txt", "content2"),
            new FileInfo("file3.txt", "content3"),
            new FileInfo("file4.txt", "content4"),
            new FileInfo("file5.txt", "content5"),
            new FileInfo("file6.txt", "content6"),
            new FileInfo("file7.txt", "content7"),
            new FileInfo("file8.txt", "content8")
        );

        PCollection<FileInfo> input = pipeline.apply("CreateInput", Create.of(files));

        PCollection<String> processed = input.apply("ProcessFiles",
            ParDo.of(new DoFn<FileInfo, String>() {
                private final Counter filesProcessed = Metrics.counter(
                    BeamLocalParallel.class, "filesProcessed");
                
                private List<String> workerFiles;
                
                @Setup
                public void setup() {
                    workerFiles = new ArrayList<>();
                    String workerName = Thread.currentThread().getName();
                    System.out.println("Worker setup: " + workerName);
                    System.out.println("Worker " + workerName + " initialized and ready to process files");
                }
                
                @StartBundle
                public void startBundle() {
                    String workerName = Thread.currentThread().getName();
                    System.out.println("\n=== Starting new bundle on worker: " + workerName + " ===");
                }
                
                @ProcessElement
                public void processElement(@Element FileInfo fileInfo, 
                                        OutputReceiver<String> out,
                                        DoFn.ProcessContext c) {
                    String workerName = Thread.currentThread().getName();
                    workerFiles.add(fileInfo.filename);
                    
                    // Get worker details
                    Runtime runtime = Runtime.getRuntime();
                    long totalMemory = runtime.totalMemory() / (1024 * 1024);
                    long freeMemory = runtime.freeMemory() / (1024 * 1024);
                    long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
                    
                    // Print detailed worker and file information
                    System.out.println(String.format(
                        "\nWorker Processing Details:\n" +
                        "------------------------\n" +
                        "Worker Thread: %s\n" +
                        "Processing File: %s\n" +
                        "Files Processed by this Worker: %s\n" +
                        "Memory Usage: %d MB used of %d MB total (%d MB free)\n" +
                        "Available Processors: %d\n" +
                        "------------------------",
                        workerName,
                        fileInfo.filename,
                        String.join(", ", workerFiles),
                        usedMemory,
                        totalMemory,
                        freeMemory,
                        runtime.availableProcessors()
                    ));
                    
                    // Increment our counter
                    filesProcessed.inc();
                    
                    // Simulate file processing
                    try {
                        Thread.sleep(1000); // Simulate work
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    
                    out.output(String.format("File '%s' processed by worker: %s", 
                        fileInfo.filename, workerName));
                }
                
                @FinishBundle
                public void finishBundle() {
                    String workerName = Thread.currentThread().getName();
                    System.out.println("\n=== Finishing bundle on worker: " + workerName);
                    System.out.println("Total files processed in this bundle by " + 
                        workerName + ": " + workerFiles.size());
                    System.out.println("Files processed in this bundle: " + 
                        String.join(", ", workerFiles));
                    System.out.println("=== Bundle complete ===\n");
                }
                
                @Teardown
                public void teardown() {
                    String workerName = Thread.currentThread().getName();
                    System.out.println("Worker teardown: " + workerName);
                    System.out.println("Total files processed by " + workerName + 
                        " during lifetime: " + workerFiles.size());
                    System.out.println("All files processed by this worker: " + 
                        String.join(", ", workerFiles) + "\n");
                }
                
                @Override
                public void populateDisplayData(DisplayData.Builder builder) {
                    builder.add(DisplayData.item("worker", Thread.currentThread().getName())
                           .withLabel("Worker Thread"));
                }
            }));

        pipeline.run().waitUntilFinish();
    }
}
