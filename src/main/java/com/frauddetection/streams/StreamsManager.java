package com.frauddetection.streams;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class StreamsManager {

    private List<KafkaStreams> streams;

    @PostConstruct
    public void start() {
        streams = FraudDetectionTopology.buildAll();
        System.out.println("StreamsManager started " + streams.size() + " topologies.");
    }

    @PreDestroy
    public void shutdown() {
        if (streams != null) {
            streams.forEach(KafkaStreams::close);
        }
    }

    public <T> ReadOnlyKeyValueStore<String, T> getStore(String storeName) {
        for (KafkaStreams ks : streams) {
            try {
                return ks.store(StoreQueryParameters.fromNameAndType(
                        storeName, QueryableStoreTypes.keyValueStore()));
            } catch (IllegalArgumentException e) {
                // store not found in this streams instance
            }
        }
        throw new IllegalArgumentException("Store not found: " + storeName);
    }
}
