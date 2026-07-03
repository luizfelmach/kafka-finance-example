package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class PasswordChangeTopology {

    public static KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(KafkaConfig.TOPIC_AUTH_EVENTS, Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
            .filter((key, auth) -> "password_change".equals(auth.getEventType()))
            .mapValues(auth -> FraudAlert.passwordChange(auth))
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.streamsProps("fraud-detection-password-change"));
        streams.start();
        return streams;
    }
}
