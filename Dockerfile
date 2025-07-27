FROM openjdk:17-jdk-slim

WORKDIR /app

# Copy Gradle wrapper scripts
COPY gradlew gradlew.bat ./
# Copy Gradle files
COPY build.gradle settings.gradle gradle.properties ./
COPY gradle/ ./gradle/

# Copy source code
COPY src/ ./src/

# Build the application
RUN ./gradlew build -x test

# Expose the application port
EXPOSE 8080

# Run the application
CMD ["java", "-jar", "build/libs/source-coordinator-0.0.1-SNAPSHOT.jar"] 