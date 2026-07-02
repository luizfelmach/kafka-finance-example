package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import org.apache.kafka.streams.StreamsBuilder;

public class FraudDetectionTopology {

    public static void build(StreamsBuilder builder) {
        HighAmountTopology.build(builder);
        BurstTopology.build(builder);
        UnknownDeviceTopology.build(builder);
        PasswordChangeTopology.build(builder);
        AccountTakeoverTopology.build(builder);
        EmptyingAccountTopology.build(builder);
        ParallelLoginTopology.build(builder);
        FarawayLoginTopology.build(builder);
        UnderObservationTopology.build(builder);
    }
}
