package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class AccountTakeoverTopology {

    public static KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(KafkaConfig.TOPIC_AUTH_EVENTS, Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
            .filter((key, auth) -> {
                int failedAttempts = auth.getRecentFailedAttempts() != null ? auth.getRecentFailedAttempts() : 0;
                return "password_change".equals(auth.getEventType()) && failedAttempts >= 3;
            })
            .mapValues(auth -> FraudAlert.accountTakeover(auth))
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.streamsProps("fraud-detection-account-takeover"));
        streams.start();
        return streams;
    }
}
