#!/bin/bash
set -e

# Log function for debugging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >&2
}

# Verify Java version and modules
verify_java() {
    local java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    log "Java Version: $java_version"
    
    # Verify Java modules are accessible
    java --list-modules | grep -q "java.base" || {
        log "ERROR: Required Java modules not available"
        return 1
    }
}

# Function to verify library dependencies
verify_libraries() {
    local missing_libs=()
    for lib in "$@"; do
        if ! ldconfig -p | grep -q "$lib"; then
            missing_libs+=("$lib")
        fi
    }
    if [ ${#missing_libs[@]} -ne 0 ]; then
        log "ERROR: Missing libraries: ${missing_libs[*]}"
        return 1
    fi
}

# Set up environment variables
setup_environment() {
    # Add custom library paths
    export LD_LIBRARY_PATH="/usr/local/lib:/usr/lib:/lib:${LD_LIBRARY_PATH:-}"
    
    # Refresh shared library cache
    ldconfig
    
    # Set other environment variables your libraries might need
    export LIBRARY_CONFIG_PATH="/opt/apache/beam/config"
    export LIBRARY_TEMP_PATH="/tmp/library"
}

# Create necessary directories
setup_directories() {
    mkdir -p /tmp/library
    mkdir -p /opt/apache/beam/config
    
    # Set proper permissions
    chmod 755 /tmp/library
    chmod 755 /opt/apache/beam/config
}

# Main startup sequence
main() {
    log "Starting worker initialization..."
    
    # Verify Java installation
    verify_java || {
        log "Java verification failed"
        exit 1
    }
    
    # Run setup steps
    setup_directories
    setup_environment
    
    # Verify system libraries
    verify_libraries "libnameprocessor.so" "libstdc++.so.6" || {
        log "Library verification failed"
        exit 1
    }
    
    log "Worker initialization complete"
    
    # Execute the original beam worker startup command
    exec /opt/apache/beam/boot "$@"
}

# Execute main function with error handling
main "$@" || {
    log "Worker startup failed"
    exit 1
}
