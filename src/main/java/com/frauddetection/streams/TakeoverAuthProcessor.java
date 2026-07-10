package com.frauddetection.streams;

import com.frauddetection.model.AuthEvent;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class TakeoverAuthProcessor implements Processor<String, AuthEvent, Void, Void> {

    private KeyValueStore<String, TakeoverState> store;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.store = context.getStateStore("takeover-store");
        context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {});
    }

    @Override
    public void process(Record<String, AuthEvent> record) {
        AuthEvent auth = record.value();
        if (auth == null) return;

        TakeoverState state = store.get(record.key());
        if (state == null) {
            state = new TakeoverState(false, false, false);
        }

        if ("login".equals(auth.getEventType())) {
            state.setLoginSeen(true);
        } else if ("password_change".equals(auth.getEventType())) {
            state.setPwChangeSeen(true);
        }

        store.put(record.key(), state);
    }

    @Override
    public void close() {}
}
