package com.example.gcsscanner;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.io.gcp.storage.GcsIO;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class EnhancedGCSFileScannerJob {
    private static final String SOURCE_BUCKET = "gs://source-bucket/";
    private static final String DESTINATION_BUCKET = "gs://destination-bucket/";

    public static class FileProcessor extends SimpleFunction<MatchResult.Metadata, Void> {
        @Override
        public Void apply(MatchResult.Metadata metadata) {
            ResourceId sourceResource = metadata.resourceId();
            String sourceFileName = sourceResource.getFilename();
            String sourceFilePath = sourceResource.toString();

            try {
                // Move file to destination bucket
                String destinationPath = moveFileToDestinationBucket(sourceResource);

                // Update database with new file metadata
                updateFileMetadata(sourceFileName, sourceFilePath, destinationPath);
            } catch (Exception e) {
                // Implement proper error handling and logging
                e.printStackTrace();
            }
            return null;
        }

        private String moveFileToDestinationBucket(ResourceId sourceResource) {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            
            // Extract source bucket and file details
            String sourceBucket = sourceResource.getFilename().split("/")[0];
            String sourceFileName = sourceResource.getFilename();

            // Generate destination path (optional: add timestamp or unique identifier)
            String destinationFileName = generateDestinationFileName(sourceFileName);
            
            // Copy file to destination bucket
            BlobId sourceBlob = BlobId.of(sourceBucket, sourceFileName);
            BlobId destinationBlob = BlobId.of(DESTINATION_BUCKET, destinationFileName);
            
            Blob blob = storage.copy(
                Storage.CopyRequest.newBuilder()
                    .setSource(sourceBlob)
                    .setTarget(destinationBlob)
                    .build()
            ).getResult();

            // Optional: Delete source file after successful copy
            storage.delete(sourceBlob);

            return DESTINATION_BUCKET + destinationFileName;
        }

        private String generateDestinationFileName(String sourceFileName) {
            // Implement custom naming logic if needed
            // Examples:
            // - Add timestamp
            // - Add unique identifier
            // - Preserve original structure
            return "processed/" + System.currentTimeMillis() + "_" + sourceFileName;
        }

        private void updateFileMetadata(String originalFileName, 
                                        String originalFilePath, 
                                        String newFilePath) throws Exception {
            try (Connection conn = getDatabaseConnection()) {
                String insertQuery = """
                    INSERT INTO file_metadata 
                    (original_filename, original_filepath, processed_filepath, status) 
                    VALUES (?, ?, ?, 'processed')
                """;
                
                try (PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {
                    pstmt.setString(1, originalFileName);
                    pstmt.setString(2, originalFilePath);
                    pstmt.setString(3, newFilePath);
                    pstmt.executeUpdate();
                }
            }
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(
            PipelineOptionsFactory.fromArgs(args).withValidation()
        );

        pipeline
            .apply("Match Files", FileIO.match().filepattern(SOURCE_BUCKET + "**"))
            .apply("Process Files", MapElements.via(new FileProcessor()));

        pipeline.run();
    }
}
