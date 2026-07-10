package com.frauddetection.streams;

import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.TransactionEvent;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class TakeoverTxProcessor implements Processor<String, TransactionEvent, String, FraudAlert> {

    private KeyValueStore<String, TakeoverState> store;
    private ProcessorContext<String, FraudAlert> context;

    @Override
    public void init(ProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.store = context.getStateStore("takeover-store");
        context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {});
    }

    @Override
    public void process(Record<String, TransactionEvent> record) {
        TransactionEvent tx = record.value();
        if (tx == null || tx.getAmount() < 5000) return;

        TakeoverState state = store.get(record.key());
        if (state == null) return;
        if (!state.isLoginSeen() || !state.isPwChangeSeen()) return;
        if (state.isAlertSent()) return;

        state.setAlertSent(true);
        store.put(record.key(), state);

        FraudAlert alert = FraudAlert.accountTakeover(tx);
        context.forward(record.withValue(alert));
    }

    @Override
    public void close() {}
}
