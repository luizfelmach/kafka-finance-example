package com.frauddetection.consumers;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.serialization.AuthEventDeserializer;
import com.frauddetection.serialization.TransactionEventDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class PasswordChangeConsumer {

    private static final long WINDOW_MS = 5 * 60 * 1000; // 5 minutos
    private static final double AMOUNT_THRESHOLD = 1000.0;

    private static final Map<String, Long> passwordChanges =
        new ConcurrentHashMap<>();

    public static void main(String[] args) {
        // Thread 1: consome auth.events
        Thread authThread = new Thread(() -> consumeAuthEvents());
        authThread.setDaemon(true);
        authThread.start();

        // Thread 2: consome transactions.raw
        consumeTransactions();
    }

    private static void consumeAuthEvents() {
        Properties props = KafkaConfig.consumerProps(
            "password-change-consumer-auth"
        );
        props.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            AuthEventDeserializer.class.getName()
        );

        KafkaConsumer<String, AuthEvent> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(
            Collections.singletonList(KafkaConfig.TOPIC_AUTH_EVENTS)
        );

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                consumer.close();
            })
        );

        while (true) {
            ConsumerRecords<String, AuthEvent> records = consumer.poll(
                Duration.ofMillis(500)
            );
            long now = System.currentTimeMillis();

            for (ConsumerRecord<String, AuthEvent> record : records) {
                AuthEvent auth = record.value();
                if (auth == null) continue;

                if ("password_change".equals(auth.getEventType())) {
                    passwordChanges.put(auth.getUserId(), now);
                    System.out.printf(
                        "[INFO] Password change detected | user=%s | event=%s%n",
                        auth.getUserId(),
                        auth.getEventId()
                    );
                }
            }
        }
    }

    private static void consumeTransactions() {
        Properties props = KafkaConfig.consumerProps(
            "password-change-consumer-tx"
        );
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
            "PasswordChangeConsumer started. Monitoring auth events and transactions..."
        );

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                System.out.println("\nShutting down PasswordChangeConsumer...");
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

                String userId = tx.getUserId();
                Long pwChangeTime = passwordChanges.get(userId);

                if (
                    pwChangeTime != null &&
                    (now - pwChangeTime) <= WINDOW_MS &&
                    tx.getAmount() > AMOUNT_THRESHOLD
                ) {
                    System.out.printf(
                        "[ALERT] PASSWORD_CHANGE_THEN_HIGH_TX | tx=%s | user=%s | amount=R$%.2f | minSincePwChange=%.1f%n",
                        tx.getTransactionId(),
                        userId,
                        tx.getAmount(),
                        (now - pwChangeTime) / 60000.0
                    );
                }
            }
        }
    }
}
