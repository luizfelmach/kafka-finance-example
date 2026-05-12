package com.frauddetection.producers;

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

public class PasswordChangeFraudProducer {

    private static final Random RANDOM = new Random();
    private static final List<String> TRANSACTION_TYPES = List.of("PIX", "CRED", "DEB");

    public static void main(String[] args) {
        List<ClientProfile> clients = ClientLoader.loadClients("clients.json");
        if (clients.isEmpty()) {
            System.err.println("No clients found");
            System.exit(1);
        }

        KafkaProducer<String, TransactionEvent> txProducer = new KafkaProducer<>(
            KafkaConfig.producerProps()
        );
        KafkaProducer<String, AuthEvent> authProducer = new KafkaProducer<>(
            KafkaConfig.producerProps()
        );

        ClientProfile client = clients.get(RANDOM.nextInt(clients.size()));

        // Step 1: password change
        String authId = "auth-" + UUID.randomUUID().toString().substring(0, 8);
        String deviceId = pickRandom(client.trustedDevices());
        AuthEvent auth = new AuthEvent(
            authId,
            client.userId(),
            "password_change",
            deviceId,
            client.homeIp()
        );
        authProducer.send(
            new ProducerRecord<>(
                KafkaConfig.TOPIC_AUTH_EVENTS,
                client.userId(),
                auth
            )
        );
        System.out.printf(
            "AUTH-> %-17s | %-15s | user=%s%n",
            auth.getEventId(),
            auth.getEventType(),
            client.userId()
        );

        try {
            Thread.sleep(2000 + RANDOM.nextInt(1001)); // 2–3s
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Step 2: login after password change
        String loginId = "auth-" + UUID.randomUUID().toString().substring(0, 8);
        AuthEvent login = new AuthEvent(
            loginId,
            client.userId(),
            "login",
            deviceId,
            client.homeIp()
        );
        authProducer.send(
            new ProducerRecord<>(
                KafkaConfig.TOPIC_AUTH_EVENTS,
                client.userId(),
                login
            )
        );
        System.out.printf(
            "AUTH-> %-17s | %-15s | user=%s%n",
            login.getEventId(),
            login.getEventType(),
            client.userId()
        );

        try {
            Thread.sleep(1000 + RANDOM.nextInt(1001)); // 1–2s
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Step 3: high value transaction
        String txId = "tx-" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = pickRandom(client.accounts());
        String type = TRANSACTION_TYPES.get(RANDOM.nextInt(TRANSACTION_TYPES.size()));
        double amount = 3000.0 + RANDOM.nextDouble() * 3000.0; // R$ 3.000 – R$ 6.000
        amount = Math.round(amount * 100.0) / 100.0;

        String destinationAccount = pickRandomDifferentAccount(client, clients);

        TransactionEvent tx = new TransactionEvent(
            txId,
            accountId,
            client.userId(),
            type,
            amount,
            deviceId,
            client.homeIp(),
            destinationAccount
        );

        txProducer.send(
            new ProducerRecord<>(
                KafkaConfig.TOPIC_TRANSACTIONS_RAW,
                client.userId(),
                tx
            )
        );
        System.out.printf(
            "TX  -> %-17s | %-7s | R$%-8s | user=%s%n",
            tx.getTransactionId(),
            tx.getType(),
            String.format("%.2f", tx.getAmount()),
            client.userId()
        );

        txProducer.close();
        authProducer.close();
        System.out.println("PasswordChangeFraudProducer finished.");
    }

    private static String pickRandom(List<String> list) {
        return list.get(RANDOM.nextInt(list.size()));
    }

    private static String pickRandomDifferentAccount(
        ClientProfile exclude,
        List<ClientProfile> allClients
    ) {
        ClientProfile other;
        do {
            other = allClients.get(RANDOM.nextInt(allClients.size()));
        } while (other == exclude);
        return pickRandom(other.accounts());
    }
}
