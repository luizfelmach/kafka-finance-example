package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

public class FarawayLoginTopology {

    public static void build(StreamsBuilder builder) {
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore("last-login-store");

        builder
            .addStateStore(Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), JsonSerdes.lastLogin()))
            .stream(KafkaConfig.TOPIC_AUTH_EVENTS, Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
            .process(() -> new FarawayTransformer(), "last-login-store")
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    }
}
