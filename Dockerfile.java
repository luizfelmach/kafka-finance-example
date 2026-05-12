FROM maven:3.9.9-eclipse-temurin-17-alpine AS builder
WORKDIR /workspace

COPY pom.xml ./
RUN mvn -q -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

COPY --from=trajano/alpine-libfaketime /faketime.so /lib/faketime.so
ENV LD_PRELOAD=/lib/faketime.so

COPY --from=builder /workspace/target/kafka-finance-example-1.0-SNAPSHOT.jar /app/kafka-finance-example.jar

CMD ["sh", "-c", "while true; do sleep 3600; done"]
