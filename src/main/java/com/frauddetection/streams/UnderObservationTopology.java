package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

public class UnderObservationTopology {

    public static void build(StreamsBuilder builder) {
        builder
            .stream(KafkaConfig.TOPIC_TRANSACTIONS_RAW, Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
            .groupByKey()
            .aggregate(
                TxHistory::new,
                (key, tx, history) -> {
                    history.add(tx);
                    return history;
                },
                Materialized.<String, TxHistory>as(Stores.persistentKeyValueStore("observation-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(JsonSerdes.txHistory())
            )
            .filter((key, history) -> {
                if (history.size() < 5) return false;
                return history.sumLastN(5).compareTo(
                    history.sumLastN(history.size()).multiply(java.math.BigDecimal.valueOf(0.9))
                ) > 0;
            })
            .toStream()
            .map((key, history) -> KeyValue.pair(
                key,
                FraudAlert.underObservation(key, key,
                    "90%+ of recent activity concentrated in last 5 transactions")
            ))
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    }
}
