package com.frauddetection.consumers;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.serialization.TransactionEventDeserializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BurstTransactionConsumer {

    private static final long WINDOW_MS = 60 * 1000; // 60 segundos
    private static final int THRESHOLD = 5;

    private static final Map<String, List<Long>> timestamps =
        new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Properties props = KafkaConfig.consumerProps("burst-consumer");
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
            "BurstTransactionConsumer started. Monitoring transactions..."
        );

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                System.out.println(
                    "\nShutting down BurstTransactionConsumer..."
                );
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

                List<Long> times = timestamps.computeIfAbsent(accountId, k ->
                    new ArrayList<>()
                );
                times.removeIf(t -> now - t > WINDOW_MS);
                times.add(now);

                if (times.size() >= THRESHOLD) {
                    System.out.printf(
                        "[ALERT] BURST_TRANSACTIONS | tx=%s | account=%s | count=%d in 60s | user=%s%n",
                        tx.getTransactionId(),
                        accountId,
                        times.size(),
                        tx.getUserId()
                    );
                }
            }
        }
    }
}
