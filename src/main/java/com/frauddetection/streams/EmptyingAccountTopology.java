package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.math.BigDecimal;

public class EmptyingAccountTopology {

    public static void build(StreamsBuilder builder) {
        builder
            .stream(KafkaConfig.TOPIC_TRANSACTIONS_RAW, Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
            .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.transactionEvent()))
            .aggregate(
                () -> BigDecimal.ZERO,
                (key, tx, balance) -> balance.subtract(BigDecimal.valueOf(tx.getAmount())),
                Materialized.with(Serdes.String(), JsonSerdes.bigDecimal())
            )
            .toStream()
            .filter((key, balance) -> balance.compareTo(BigDecimal.ZERO) < 0)
            .mapValues(balance -> FraudAlert.emptyingAccount("Account drained. Balance: " + balance))
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    }
}
