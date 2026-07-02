package com.frauddetection.web;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.streams.FraudDetectionTopology;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(scanBasePackages = "com.frauddetection")
public class FraudDetectionApplication {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(FraudDetectionApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    public KafkaStreams fraudDetectionStreams() {
        Properties props = KafkaConfig.streamsProps("fraud-detection-app");
        StreamsBuilder builder = new StreamsBuilder();
        FraudDetectionTopology.build(builder);
        KafkaStreams streams = new KafkaStreams(builder.build(props), props);

        streams.setStateListener((newState, oldState) ->
            log.info("KafkaStreams state changed from {} to {}", oldState, newState));

        streams.setUncaughtExceptionHandler((thread, throwable) ->
            log.error("KafkaStreams uncaught exception on thread {}: {}", thread.getName(), throwable.getMessage(), throwable));

        streams.start();
        log.info("KafkaStreams started, initial state: {}", streams.state());
        return streams;
    }
}
