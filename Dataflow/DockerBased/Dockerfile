# Use the official Apache Beam JDK 17 base image
FROM apache/beam_java17_sdk:2.54.0

# Set shell options for better error handling
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install system dependencies and development tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libssl-dev \
    curl \
    wget \
    git \
    # Add your specific library dependencies here
    libsomelib-dev \
    libotherlib-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set up working directory
WORKDIR /opt/apache/beam

# Copy your native libraries and dependencies
COPY native-libs/ /usr/local/lib/
RUN ldconfig

# Copy your configuration files if any
COPY config/ /opt/apache/beam/config/

# Copy the worker startup script
COPY worker-startup.sh /opt/apache/beam/
RUN chmod +x /opt/apache/beam/worker-startup.sh

# Set environment variables
ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/lib:/usr/lib:/lib:${LD_LIBRARY_PATH:-}"

# Set JVM options for Java 17
ENV JAVA_TOOL_OPTIONS="\
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    -XX:+UseG1GC \
    -XX:+UseStringDeduplication"

# Verify Java version
RUN java -version

# The startup script will be executed by Dataflow
ENTRYPOINT ["/opt/apache/beam/worker-startup.sh"]
