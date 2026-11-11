# Build the executable JAR with ./mvnw clean verify before building this image.
FROM eclipse-temurin:25-jre@sha256:ecdc0adb37bde1a281c4d245a3cd535004b1fc8322d560f5b09463b248c81df0

WORKDIR /app
COPY --chown=10001:10001 target/mqtt-kafka-bridge-*.jar /app/app.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
