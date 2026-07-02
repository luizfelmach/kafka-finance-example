package com.frauddetection.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.utils.ClientLoader;
import com.frauddetection.utils.ClientProfile;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class AccountTakeoverConsumerProducer {

    private static final long WINDOW_MS = 10 * 60 * 1000; // 10 minutos
    private static final double HIGH_VALUE_THRESHOLD = 1000.0;

    private static final Map<String, UserState> userStates = new ConcurrentHashMap<>();
    private static final Map<String, List<String>> trustedDevicesMap = new HashMap<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        List<ClientProfile> clients = ClientLoader.loadClients("clients.json");
        for (ClientProfile c : clients) {
            trustedDevicesMap.put(c.userId(), c.trustedDevices());
        }

        String groupId = args.length > 0 ? args[0] : "account-takeover-group";
        Properties consumerProps = KafkaConfig.consumerProps(groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, FraudAlert> fraudProducer = new KafkaProducer<>(KafkaConfig.producerProps());

        consumer.subscribe(Arrays.asList(KafkaConfig.TOPIC_AUTH_EVENTS, KafkaConfig.TOPIC_TRANSACTIONS_RAW));

        System.out.println("AccountTakeoverConsumerProducer started. Monitoring transactions...");

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
                        if (record.topic().equals(KafkaConfig.TOPIC_AUTH_EVENTS)) {
                            AuthEvent auth = objectMapper.readValue(record.value(), AuthEvent.class);
                            handleAuthEvent(auth);
                        } 
                        else if (record.topic().equals(KafkaConfig.TOPIC_TRANSACTIONS_RAW)) {
                            TransactionEvent tx = objectMapper.readValue(record.value(), TransactionEvent.class);
                            handleTransactionEvent(tx, fraudProducer);
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

    private static void handleAuthEvent(AuthEvent auth) {
        if (auth == null) return;

        String userId = auth.getUserId();
        long now = System.currentTimeMillis();
        List<String> trusted = trustedDevicesMap.get(userId);
        boolean isUnknownDevice = trusted == null || !trusted.contains(auth.getDeviceId());

        if ("login".equals(auth.getEventType()) && isUnknownDevice) {
            UserState state = userStates.computeIfAbsent(userId, k -> new UserState());
            state.unknownLoginTime = now;
        } 
        else if ("password_change".equals(auth.getEventType())) {
            UserState state = userStates.get(userId);
            if (state != null && state.unknownLoginTime > 0 && (now - state.unknownLoginTime) <= WINDOW_MS) {
                state.passwordChangeTime = now;
                System.out.printf("[INFO] Password change after suspect login | user=%s%n", userId);
            }
        }
    }

    private static void handleTransactionEvent(TransactionEvent tx, KafkaProducer<String, FraudAlert> producer) {
        if (tx == null) return;

        String userId = tx.getUserId();
        UserState state = userStates.get(userId);
        long now = System.currentTimeMillis();

        if (state != null && state.passwordChangeTime > 0) {
            boolean withinWindow = (now - state.passwordChangeTime) <= WINDOW_MS;
            boolean isHighValue = tx.getAmount() >= HIGH_VALUE_THRESHOLD;

            if (withinWindow && isHighValue) {
                String desc = String.format(
                    "Account takeover: Suspect login -> Password change -> High value transaction (R$%.2f)", 
                    tx.getAmount()
                );
                
                FraudAlert alert = new FraudAlert("alert-" + UUID.randomUUID().toString().substring(0, 8), tx.getAccountId(), "HIGH", "ACCOUNT_TAKEOVER", userId, desc, now);
                producer.send(new ProducerRecord<>(KafkaConfig.TOPIC_FRAUD_EVENTS, userId, alert));

                System.out.printf("[ALERT] ACCOUNT_TAKEOVER | user=%s | amount=R$%.2f%n", userId, tx.getAmount());

                userStates.remove(userId);
            }
        }
    }

    private static class UserState {
        long unknownLoginTime = 0;
        long passwordChangeTime = 0;
    }
}