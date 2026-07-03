package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class HighAmountTopology {

    private static final double LIMIT = 50000.00;

    public static KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(KafkaConfig.TOPIC_TRANSACTIONS_RAW, Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
            .filter((key, tx) -> tx.getAmount() > LIMIT)
            .mapValues(tx -> FraudAlert.highValue(tx))
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.streamsProps("fraud-detection-high-amount"));
        streams.start();
        return streams;
    }
}
