package com.frauddetection.streams;

import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.GeoLocation;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class FarawayTransformer implements Processor<String, AuthEvent, String, FraudAlert> {

    private KeyValueStore<String, LastLogin> store;
    private ProcessorContext<String, FraudAlert> context;

    @Override
    public void init(ProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.store = context.getStateStore("last-login-store");
        context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {});
    }

    @Override
    public void process(Record<String, AuthEvent> record) {
        AuthEvent auth = record.value();
        if (auth == null || auth.getGeoLocation() == null) return;

        LastLogin last = store.get(record.key());
        if (last != null && last.getGeoLocation() != null) {
            double distance = haversine(last.getGeoLocation(), auth.getGeoLocation());
            long timeDiff = auth.getTimestamp() - last.getTimestamp();

            if (timeDiff > 0) {
                double speedKmh = (distance / 1000.0) / (timeDiff / 3600.0);
                if (speedKmh > 500.0) {
                    FraudAlert alert = FraudAlert.farawayLogin(auth, distance, speedKmh);
                    context.forward(record.withValue(alert));
                }
            }
        }

        store.put(record.key(), new LastLogin(auth.getTimestamp(), auth.getGeoLocation()));
    }

    @Override
    public void close() {}

    private double haversine(GeoLocation g1, GeoLocation g2) {
        double R = 6371000;
        double dLat = Math.toRadians(g2.getLatitude() - g1.getLatitude());
        double dLon = Math.toRadians(g2.getLongitude() - g1.getLongitude());
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                 + Math.cos(Math.toRadians(g1.getLatitude())) * Math.cos(Math.toRadians(g2.getLatitude()))
                 * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
