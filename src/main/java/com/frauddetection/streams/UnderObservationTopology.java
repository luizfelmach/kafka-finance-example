package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class UnderObservationTopology {

    public static void build(StreamsBuilder builder) {
        KTable<String, FraudAlert> underObservation = builder
            .stream(KafkaConfig.TOPIC_TRANSACTIONS_RAW, Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
            .groupByKey()
            .aggregate(
                TxHistory::new,
                (key, tx, history) -> {
                    history.add(tx);
                    return history;
                },
                Materialized.with(Serdes.String(), JsonSerdes.txHistory())
            )
            .filter((key, history) -> {
                if (history.size() < 5) return false;
                return history.sumLastN(5).compareTo(
                    history.sumLastN(history.size()).multiply(java.math.BigDecimal.valueOf(0.8))
                ) > 0;
            })
            .mapValues(history -> FraudAlert.underObservation(
                "90%+ of recent activity concentrated in last 5 transactions"));

        underObservation
            .toStream()
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    }
}
