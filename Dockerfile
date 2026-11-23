
# Build stage
FROM maven:3.9.9-eclipse-temurin-21 AS builder

WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src

RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=builder /app/target/telemetry-engine-0.1.0-SNAPSHOT.jar app.jar

EXPOSE 50051

ENTRYPOINT ["java", "-Xmx160m", "-XX:MaxDirectMemorySize=64m", "-jar", "app.jar"]

