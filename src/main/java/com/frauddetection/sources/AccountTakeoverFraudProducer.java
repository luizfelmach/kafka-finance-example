package com.frauddetection.sources;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.utils.ClientLoader;
import com.frauddetection.utils.ClientProfile;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AccountTakeoverFraudProducer {

    private static final Random RANDOM = new Random();
    private static final String UNKNOWN_DEVICE = "dev-TAKEOVER-99";

    public static void main(String[] args) {
        List<ClientProfile> clients = ClientLoader.loadClients("clients.json");
        if (clients.isEmpty()) {
            System.err.println("No clients found");
            System.exit(1);
        }

        KafkaProducer<String, TransactionEvent> txProducer = new KafkaProducer<>(KafkaConfig.producerProps());
        KafkaProducer<String, AuthEvent> authProducer = new KafkaProducer<>(KafkaConfig.producerProps());

        ClientProfile client = clients.get(RANDOM.nextInt(clients.size()));
        String userId = client.userId();

        System.out.println("Starting simulation for user: " + userId);

        double lat = Math.round((-5.0 - RANDOM.nextDouble() * 25.0) * 100.0) / 100.0;
        double lng = Math.round((-35.0 - RANDOM.nextDouble() * 20.0) * 100.0) / 100.0;

        // First step -> The thief logs in from an unknown device
        AuthEvent login = new AuthEvent(
                "auth-" + UUID.randomUUID().toString().substring(0, 8),
                userId,
                "login",
                UNKNOWN_DEVICE,
                "1.2.3.4",
                lat,
                lng,
                System.currentTimeMillis());
        authProducer.send(new ProducerRecord<>(KafkaConfig.TOPIC_AUTH_EVENTS, userId, login));
        System.out.println("Sent: Unknown device login");

        sleep(2000);

        // Second step -> The thief changes the password to prevent the real user from
        // logging in
        AuthEvent pwChange = new AuthEvent(
                "auth-" + UUID.randomUUID().toString().substring(0, 8),
                userId,
                "password_change",
                UNKNOWN_DEVICE,
                "1.2.3.4",
                lat,
                lng,
                System.currentTimeMillis());
        authProducer.send(new ProducerRecord<>(KafkaConfig.TOPIC_AUTH_EVENTS, userId, pwChange));
        System.out.println("Sent: Password change");

        sleep(2000);

        // Third step -> The thief makes a high value transaction
        TransactionEvent tx = new TransactionEvent(
                "tx-" + UUID.randomUUID().toString().substring(0, 8),
                client.accounts().get(0),
                userId,
                "PIX",
                5500.0,
                UNKNOWN_DEVICE,
                "1.2.3.4",
                "account-DEST-123",
                System.currentTimeMillis());
        txProducer.send(new ProducerRecord<>(KafkaConfig.TOPIC_TRANSACTIONS_RAW, userId, tx));
        System.out.println("Sent: High value transaction (R$ 5500.00)");

        txProducer.close();
        authProducer.close();
        System.out.println("Simulation finished.");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
