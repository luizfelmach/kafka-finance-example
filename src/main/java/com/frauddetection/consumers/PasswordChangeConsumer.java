package com.frauddetection.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.TransactionEvent;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PasswordChangeConsumer {

    private static final long WINDOW_MS = 5 * 60 * 1000;
    private static final double AMOUNT_THRESHOLD = 1000.0;

    private static final Map<String, Long> passwordChanges = new ConcurrentHashMap<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        String groupId = args.length > 0 ? args[0] : "password-change-consumer";

        Properties props = KafkaConfig.consumerProps(groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(KafkaConfig.TOPIC_AUTH_EVENTS, KafkaConfig.TOPIC_TRANSACTIONS_RAW));

        System.out.println("PasswordChangeConsumer started. Monitoring auth events and transactions...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down PasswordChangeConsumer...");
            consumer.wakeup();
        }));

        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                long now = System.currentTimeMillis();

                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        if (record.topic().equals(KafkaConfig.TOPIC_AUTH_EVENTS)) {
                            AuthEvent auth = objectMapper.readValue(record.value(), AuthEvent.class);
                            if (auth == null) continue;

                            if ("password_change".equals(auth.getEventType())) {
                                passwordChanges.put(auth.getUserId(), now);
                                System.out.printf(
                                    "[INFO] Password change detected | user=%s | event=%s%n",
                                    auth.getUserId(),
                                    auth.getEventId()
                                );
                            }
                        } else if (record.topic().equals(KafkaConfig.TOPIC_TRANSACTIONS_RAW)) {
                            TransactionEvent tx = objectMapper.readValue(record.value(), TransactionEvent.class);
                            if (tx == null) continue;

                            String userId = tx.getUserId();
                            Long pwChangeTime = passwordChanges.get(userId);

                            if (pwChangeTime != null
                                    && (now - pwChangeTime) <= WINDOW_MS
                                    && tx.getAmount() > AMOUNT_THRESHOLD) {
                                System.out.printf(
                                    "[ALERT] PASSWORD_CHANGE_THEN_HIGH_TX | tx=%s | user=%s | amount=R$%.2f | minSincePwChange=%.1f%n",
                                    tx.getTransactionId(),
                                    userId,
                                    tx.getAmount(),
                                    (now - pwChangeTime) / 60000.0
                                );
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing record from topic " + record.topic() + ": " + e.getMessage());
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
        } finally {
            consumer.close();
        }
    }
}
