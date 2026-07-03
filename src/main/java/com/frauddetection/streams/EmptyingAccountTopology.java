package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import java.math.BigDecimal;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;

public class EmptyingAccountTopology {

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(10);
    private static final Duration GRACE_PERIOD = Duration.ofMinutes(2);
    private static final BigDecimal MIN_NEGATIVE_BALANCE = new BigDecimal("-1000");
    private static final long MIN_COUNT = 3;

    public static KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(KafkaConfig.TOPIC_TRANSACTIONS_RAW, Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
            .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.transactionEvent()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(WINDOW_SIZE, GRACE_PERIOD))
            .aggregate(
                AccountAggregate::new,
                (key, tx, agg) -> agg.add(tx),
                Materialized.with(Serdes.String(), JsonSerdes.accountAggregate())
            )
            .toStream()
            .filter((windowedKey, agg) ->
                agg.getTotalAmount().compareTo(MIN_NEGATIVE_BALANCE) < 0
                && agg.getCount() >= MIN_COUNT
            )
            .map((windowedKey, agg) -> KeyValue.pair(
                windowedKey.key(),
                FraudAlert.emptyingAccount(
                    agg.getLastAccountId(),
                    agg.getLastUserId(),
                    "Account drained. Balance: " + agg.getTotalAmount()
                    + " in " + agg.getCount() + " transactions"
                )
            ))
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.streamsProps("fraud-detection-emptying-account"));
        streams.start();
        return streams;
    }
}
