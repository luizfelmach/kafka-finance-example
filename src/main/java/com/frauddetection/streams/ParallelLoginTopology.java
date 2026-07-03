package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;

import java.time.Duration;

public class ParallelLoginTopology {

    public static KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(KafkaConfig.TOPIC_AUTH_EVENTS, Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
            .filter((key, auth) -> "login".equals(auth.getEventType()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(10)))
            .count()
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .filter((windowedKey, count) -> count > 1)
            .map((windowedKey, count) -> KeyValue.pair(
                windowedKey.key(),
                FraudAlert.parallelLogin(windowedKey.key(), "Parallel login detected: " + count + " sessions")
            ))
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.streamsProps("fraud-detection-parallel-login"));
        streams.start();
        return streams;
    }
}
