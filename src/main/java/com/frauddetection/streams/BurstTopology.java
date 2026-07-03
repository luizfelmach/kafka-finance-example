package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class BurstTopology {

    public static KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(KafkaConfig.TOPIC_TRANSACTIONS_RAW, Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .count()
            .toStream()
            .filter((windowedKey, count) -> count >= 5)
            .map((windowedKey, count) -> KeyValue.pair(
                windowedKey.key(),
                FraudAlert.transactionBurst(windowedKey.key(), count, windowedKey.window().start(), windowedKey.window().end())
            ))
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.streamsProps("fraud-detection-burst"));
        streams.start();
        return streams;
    }
}
