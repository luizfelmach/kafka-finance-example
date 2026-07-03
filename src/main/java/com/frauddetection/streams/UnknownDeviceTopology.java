package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import com.frauddetection.utils.ClientProfile;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Produced;

public class UnknownDeviceTopology {

    public static KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, ClientProfile> profiles = builder.globalTable(
            KafkaConfig.TOPIC_CLIENTS_PROFILES,
            Consumed.with(Serdes.String(), JsonSerdes.clientProfile())
        );

        builder
            .stream(KafkaConfig.TOPIC_AUTH_EVENTS, Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
            .filter((key, auth) -> auth.getDeviceId() != null)
            .join(profiles,
                (key, auth) -> auth.getUserId(),
                (auth, profile) -> profile.trustedDevices().contains(auth.getDeviceId()) ? null : auth)
            .filter((key, auth) -> auth != null)
            .mapValues(auth -> FraudAlert.unknownDevice(auth))
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.streamsProps("fraud-detection-unknown-device"));
        streams.start();
        return streams;
    }
}
