package com.frauddetection.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.TransactionEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class EmptyingAccountConsumerProducer {

    private static final long BURST_WINDOW_MS = 60 * 1000;     // 1 minuto
    private static final double HIGH_VALUE_THRESHOLD = 1000.0;
    private static final int BURST_THRESHOLD = 5;

    private static final Map<String, List<Long>> recentHighValueTxTimes = new ConcurrentHashMap<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties consumerProps = KafkaConfig.consumerProps("emptying-account-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, FraudAlert> fraudProducer = new KafkaProducer<>(KafkaConfig.producerProps());

        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_TRANSACTIONS_RAW));

        System.out.println("EmptyingAccountConsumerProducer started. Monitoring transactions...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down processor...");
            consumer.wakeup();
            fraudProducer.close();
        }));

        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        TransactionEvent tx = objectMapper.readValue(record.value(), TransactionEvent.class);
                        handleTransactionEvent(tx, fraudProducer);
                    } catch (Exception e) {
                        System.err.println("Error processing record from topic " + record.topic() + ": " + e.getMessage());
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // expected on shutdown
        } finally {
            consumer.close();
        }
    }

    private static void handleTransactionEvent(TransactionEvent tx, KafkaProducer<String, FraudAlert> producer) {
        if (tx == null || tx.getUserId() == null) return;

        String userId = tx.getUserId();
        long now = System.currentTimeMillis();

        if (tx.getAmount() >= HIGH_VALUE_THRESHOLD) {
            List<Long> times = recentHighValueTxTimes.computeIfAbsent(userId, k -> new ArrayList<>());
            times.add(now);
            times.removeIf(t -> now - t > BURST_WINDOW_MS);

            if (times.size() >= BURST_THRESHOLD) {
                String desc = String.format(
                    "Emptying Account detected: %d high-value transactions (>= R$%.2f) in 1 minute",
                    times.size(), HIGH_VALUE_THRESHOLD
                );
                
                FraudAlert alert = new FraudAlert("EMPTYING_ACCOUNT", userId, desc, now);
                producer.send(new ProducerRecord<>(KafkaConfig.TOPIC_FRAUD_EVENTS, userId, alert));

                System.out.printf("[ALERT] EMPTYING_ACCOUNT | user=%s | count=%d | last_amount=R$%.2f%n", 
                    userId, times.size(), tx.getAmount());
                
                recentHighValueTxTimes.remove(userId);
            }
        }
    }
}
