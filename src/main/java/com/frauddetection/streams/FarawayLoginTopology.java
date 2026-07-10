package com.frauddetection.streams;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.GeoLocation;
import com.frauddetection.serialization.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;

public class FarawayLoginTopology {

    public static KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();

        builder
            .stream(KafkaConfig.TOPIC_AUTH_EVENTS, Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
            .filter((key, auth) -> "login".equals(auth.getEventType()))
            .groupByKey()
            .aggregate(
                LoginPair::new,
                (key, auth, pair) -> {
                    pair.setPrevious(pair.getCurrent());
                    pair.setCurrent(auth);
                    return pair;
                },
                Materialized.<String, LoginPair>as(Stores.persistentKeyValueStore("last-login-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(JsonSerdes.loginPair())
            )
            .toStream()
            .flatMap((key, pair) -> {
                if (pair.getPrevious() == null) return Collections.emptyList();
                AuthEvent prev = pair.getPrevious();
                AuthEvent curr = pair.getCurrent();
                if (prev.getGeoLocation() == null || curr.getGeoLocation() == null) return Collections.emptyList();
                double distance = haversine(prev.getGeoLocation(), curr.getGeoLocation());
                long timeDiff = curr.getTimestamp() - prev.getTimestamp();
                if (timeDiff <= 0) return Collections.emptyList();
                double speedKmh = (distance / 1000.0) / (timeDiff / 3600.0);
                if (speedKmh > 900) {
                    return Collections.singletonList(
                        KeyValue.pair(key, FraudAlert.farawayLogin(curr, distance, speedKmh))
                    );
                }
                return Collections.emptyList();
            })
            .to(KafkaConfig.TOPIC_FRAUD_EVENTS, Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaConfig.streamsProps("fraud-detection-faraway-login"));
        streams.start();
        return streams;
    }

    private static double haversine(GeoLocation g1, GeoLocation g2) {
        double R = 6371000;
        double dLat = Math.toRadians(g2.getLatitude() - g1.getLatitude());
        double dLon = Math.toRadians(g2.getLongitude() - g1.getLongitude());
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                 + Math.cos(Math.toRadians(g1.getLatitude())) * Math.cos(Math.toRadians(g2.getLatitude()))
                 * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
