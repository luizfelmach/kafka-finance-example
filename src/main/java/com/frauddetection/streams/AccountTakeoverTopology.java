package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

public class AccountTakeoverTopology {

    public static KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, TakeoverState> authState = builder
            .stream(KafkaConfig.TOPIC_AUTH_EVENTS, Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
            .groupByKey()
            .aggregate(
                TakeoverState::new,
                (key, auth, state) -> {
                    if ("login".equals(auth.getEventType())) state.setLoginSeen(true);
                    if ("password_change".equals(auth.getEventType())) state.setPwChangeSeen(true);
                    return state;
                },
                Materialized.<String, TakeoverState>as(Stores.persistentKeyValueStore("takeover-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(JsonSerdes.takeoverState())
            );

        builder
            .stream(KafkaConfig.TOPIC_TRANSACTIONS_RAW, Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
            .filter((key, tx) -> tx.getAmount() >= 5000)
            .join(authState, (tx, state) -> {
                if (state.isLoginSeen() && state.isPwChangeSeen()) {
                    return FraudAlert.accountTakeover(tx);
                }
                return null;
            })
            .filter((key, alert) -> alert != null)
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.streamsProps("fraud-detection-account-takeover"));
        streams.start();
        return streams;
    }
}
