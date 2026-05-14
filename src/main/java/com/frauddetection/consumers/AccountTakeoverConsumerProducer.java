package com.frauddetection.consumers;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.serialization.AuthEventDeserializer;
import com.frauddetection.serialization.TransactionEventDeserializer;
import com.frauddetection.utils.ClientLoader;
import com.frauddetection.utils.ClientProfile;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AccountTakeoverConsumerProducer {

    private static final long WINDOW_MS = 10 * 60 * 1000; // 10 minutes
    private static final double HIGH_VALUE_THRESHOLD = 1000.0;

    private static final Map<String, UserState> userStates = new ConcurrentHashMap<>();
    private static Map<String, List<String>> trustedDevicesMap = new HashMap<>();

    private static KafkaProducer<String, FraudAlert> fraudProducer;

    public static void main(String[] args) {
        List<ClientProfile> clients = ClientLoader.loadClients("clients.json");
        for (ClientProfile c : clients) {
            trustedDevicesMap.put(c.userId(), c.trustedDevices());
        }

        fraudProducer = new KafkaProducer<>(KafkaConfig.producerProps());

        Thread authThread = new Thread(AccountTakeoverConsumerProducer::consumeAuthEvents);
        authThread.setDaemon(true);
        authThread.start();

        consumeTransactions();
    }

    private static void consumeAuthEvents() {
        Properties props = KafkaConfig.consumerProps("account-takeover-auth-consumer");
        props.put(
                org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                AuthEventDeserializer.class.getName());

        KafkaConsumer<String, AuthEvent> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_AUTH_EVENTS));

        try {
            while (true) {
                ConsumerRecords<String, AuthEvent> records = consumer.poll(Duration.ofMillis(500));
                long now = System.currentTimeMillis();

                for (ConsumerRecord<String, AuthEvent> record : records) {
                    AuthEvent auth = record.value();
                    if (auth == null)
                        continue;

                    String userId = auth.getUserId();
                    List<String> trusted = trustedDevicesMap.get(userId);
                    boolean isUnknownDevice = trusted == null || !trusted.contains(auth.getDeviceId());

                    if ("login".equals(auth.getEventType()) && isUnknownDevice) {
                        UserState state = userStates.computeIfAbsent(userId, k -> new UserState());
                        state.unknownLoginTime = now;
                    } else if ("password_change".equals(auth.getEventType())) {
                        UserState state = userStates.get(userId);
                        if (state != null && (now - state.unknownLoginTime) <= WINDOW_MS) {
                            state.passwordChangeTime = now;
                            System.out.printf("[INFO] Password change after unknown login | user=%s%n", userId);
                        }
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // expected
        } finally {
            consumer.close();
        }
    }

    private static void consumeTransactions() {
        Properties props = KafkaConfig.consumerProps("account-takeover-tx-consumer");
        props.put(
                org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                TransactionEventDeserializer.class.getName());

        KafkaConsumer<String, TransactionEvent> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_TRANSACTIONS_RAW));

        System.out.println("AccountTakeoverConsumerProducer started. Monitoring sequence...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down AccountTakeoverConsumerProducer...");
            consumer.wakeup();
            fraudProducer.close();
        }));

        try {
            while (true) {
                ConsumerRecords<String, TransactionEvent> records = consumer.poll(Duration.ofMillis(500));
                long now = System.currentTimeMillis();

                for (ConsumerRecord<String, TransactionEvent> record : records) {
                    TransactionEvent tx = record.value();
                    if (tx == null)
                        continue;

                    String userId = tx.getUserId();
                    UserState state = userStates.get(userId);

                    if (state != null && state.passwordChangeTime > 0 &&
                            (now - state.passwordChangeTime) <= WINDOW_MS &&
                            tx.getAmount() >= HIGH_VALUE_THRESHOLD) {

                        String description = String.format(
                                "Account takeover detected: Unknown device login followed by password change and high value transaction (R$%.2f)",
                                tx.getAmount());
                        FraudAlert alert = new FraudAlert("ACCOUNT_TAKEOVER", userId, description, now);

                        fraudProducer.send(new ProducerRecord<>(KafkaConfig.TOPIC_FRAUD_EVENTS, userId, alert));

                        System.out.printf("[ALERT] ACCOUNT_TAKEOVER | user=%s | tx=%s | amount=R$%.2f%n",
                                userId, tx.getTransactionId(), tx.getAmount());

                        // Reset state after alert to avoid duplicate alerts for the same sequence
                        userStates.remove(userId);
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
        } finally {
            consumer.close();
        }
    }

    private static class UserState {
        long unknownLoginTime = 0;
        long passwordChangeTime = 0;
    }
}
