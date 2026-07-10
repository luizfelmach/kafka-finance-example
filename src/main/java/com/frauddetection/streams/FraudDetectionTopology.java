package com.frauddetection.streams;

import org.apache.kafka.streams.KafkaStreams;

import java.util.List;

public class FraudDetectionTopology {

    public static List<KafkaStreams> buildAll() {
        return List.of(
            HighAmountTopology.build(),
            BurstTopology.build(),
            UnknownDeviceTopology.build(),
            PasswordChangeTopology.build(),
            AccountTakeoverTopology.build(),
            EmptyingAccountTopology.build(),
            FarawayLoginTopology.build()
        );
    }
}
