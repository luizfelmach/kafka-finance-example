package com.frauddetection.streams;

import org.apache.kafka.streams.KafkaStreams;

import java.util.List;

public class FraudDetectionApp {

    public static void main(String[] args) {
        List<KafkaStreams> allStreams = FraudDetectionTopology.buildAll();
        System.out.println("FraudDetectionApp started with " + allStreams.size() + " topologies.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down all topologies...");
            allStreams.forEach(KafkaStreams::close);
        }));
    }
}
