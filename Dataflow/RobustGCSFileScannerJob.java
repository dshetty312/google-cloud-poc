import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class RobustGCSFileScannerJob {
    private static final Logger logger = LoggerFactory.getLogger(RobustGCSFileScannerJob.class);
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;

    public static class FileProcessor extends SimpleFunction<MatchResult.Metadata, Void> {
        @Override
        public Void apply(MatchResult.Metadata metadata) {
            String sourceFileName = metadata.resourceId().getFilename();
            String sourceFilePath = metadata.resourceId().toString();

            try {
                processFileWithRetry(metadata.resourceId());
            } catch (Exception e) {
                logger.error("Critical failure processing file: {}", sourceFileName, e);
                // Log to error tracking system or dead-letter queue
                recordFailedFile(sourceFileName, sourceFilePath, e);
            }
            return null;
        }

        private void processFileWithRetry(ResourceId sourceResource) throws Exception {
            for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                try {
                    String destinationPath = moveFileToDestinationBucket(sourceResource);
                    updateFileMetadataTransactional(sourceResource, destinationPath);
                    return; // Success, exit method
                } catch (Exception e) {
                    if (attempt == MAX_RETRIES) {
                        throw e; // Rethrow on final attempt
                    }
                    logger.warn("Retry attempt {} for file: {}", attempt, sourceResource.getFilename());
                    Thread.sleep(RETRY_DELAY_MS * attempt);
                }
            }
        }

        private void updateFileMetadataTransactional(ResourceId sourceResource, String destinationPath) {
            Connection conn = null;
            try {
                conn = getDatabaseConnection();
                conn.setAutoCommit(false); // Start transaction

                // Prepare and execute database operations
                try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO file_metadata (...) VALUES (...)")) {
                    // Set parameters
                    pstmt.executeUpdate();
                }

                conn.commit(); // Commit transaction
            } catch (SQLException e) {
                // Rollback in case of any database error
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException rollbackEx) {
                        logger.error("Transaction rollback failed", rollbackEx);
                    }
                }
                throw new RuntimeException("Database transaction failed", e);
            } finally {
                // Restore default commit behavior
                if (conn != null) {
                    try {
                        conn.setAutoCommit(true);
                        conn.close();
                    } catch (SQLException e) {
                        logger.error("Connection close error", e);
                    }
                }
            }
        }

        private void recordFailedFile(String fileName, String filePath, Exception e) {
            try (Connection conn = getDatabaseConnection()) {
                try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO failed_file_logs (filename, filepath, error_message) VALUES (?, ?, ?)")) {
                    pstmt.setString(1, fileName);
                    pstmt.setString(2, filePath);
                    pstmt.setString(3, e.toString());
                    pstmt.executeUpdate();
                }
            } catch (SQLException dbError) {
                logger.error("Failed to log error file", dbError);
            }
        }
    }
}
