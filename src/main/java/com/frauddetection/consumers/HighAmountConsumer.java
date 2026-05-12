package com.frauddetection.consumers;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.serialization.TransactionEventDeserializer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class HighAmountConsumer {

    private static final long WINDOW_MS = 5 * 60 * 1000; // 5 minutos
    private static final double MULTIPLIER = 10.0;
    private static final double FIRST_TX_THRESHOLD = 8000.0;

    private static final Map<String, List<TxSnapshot>> history =
        new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Properties props = KafkaConfig.consumerProps("high-amount-consumer");
        props.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            TransactionEventDeserializer.class.getName()
        );

        KafkaConsumer<String, TransactionEvent> consumer = new KafkaConsumer<>(
            props
        );
        consumer.subscribe(
            Collections.singletonList(KafkaConfig.TOPIC_TRANSACTIONS_RAW)
        );

        System.out.println(
            "HighAmountConsumer started. Monitoring transactions..."
        );

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                System.out.println("\nShutting down HighAmountConsumer...");
                consumer.close();
            })
        );

        while (true) {
            ConsumerRecords<String, TransactionEvent> records = consumer.poll(
                Duration.ofMillis(500)
            );
            long now = System.currentTimeMillis();

            for (ConsumerRecord<String, TransactionEvent> record : records) {
                TransactionEvent tx = record.value();
                if (tx == null) continue;

                String accountId = tx.getAccountId();
                double amount = tx.getAmount();

                List<TxSnapshot> txs = history.computeIfAbsent(accountId, k ->
                    new ArrayList<>()
                );

                // Limpa transações antigas
                txs.removeIf(s -> now - s.timestamp > WINDOW_MS);

                // Calcula média
                double avg = txs
                    .stream()
                    .mapToDouble(s -> s.amount)
                    .average()
                    .orElse(0.0);

                boolean alert = false;
                String reason = "";

                if (txs.isEmpty() && amount > FIRST_TX_THRESHOLD) {
                    alert = true;
                    reason = "first_tx_above_threshold";
                } else if (!txs.isEmpty() && amount > avg * MULTIPLIER) {
                    alert = true;
                    reason = "above_moving_average";
                }

                if (alert) {
                    System.out.printf(
                        "[ALERT] HIGH_AMOUNT | tx=%s | account=%s | amount=R$%.2f | avg=R$%.2f | reason=%s | user=%s%n",
                        tx.getTransactionId(),
                        accountId,
                        amount,
                        avg,
                        reason,
                        tx.getUserId()
                    );
                }
                txs.add(new TxSnapshot(amount, now));
            }
        }
    }

    private record TxSnapshot(double amount, long timestamp) {}
}
