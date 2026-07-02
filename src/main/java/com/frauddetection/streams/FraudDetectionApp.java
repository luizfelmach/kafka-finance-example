package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class FraudDetectionApp {

    public static void main(String[] args) {
        Properties props = KafkaConfig.streamsProps("fraud-detection-app");

        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        props.put(StreamsConfig.producerPrefix(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);

        StreamsBuilder builder = new StreamsBuilder();
        FraudDetectionTopology.build(builder);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down FraudDetectionApp...");
            streams.close();
        }));

        streams.start();
        System.out.println("FraudDetectionApp started with all 9 detection topologies.");
    }
}
