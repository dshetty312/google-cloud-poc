CREATE TABLE failed_file_logs (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255),
    filepath VARCHAR(1024),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
