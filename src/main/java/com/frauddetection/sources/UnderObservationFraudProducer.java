package com.frauddetection.sources;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.utils.ClientLoader;
import com.frauddetection.utils.ClientProfile;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class UnderObservationFraudProducer {

    private static final Random RANDOM = new Random();
    private static final List<String> TRANSACTION_TYPES = List.of("PIX", "CRED", "DEB");

    public static void main(String[] args) {
        List<ClientProfile> clients = ClientLoader.loadClients("clients.json");
        if (clients.isEmpty()) {
            System.err.println("No clients found");
            System.exit(1);
        }

        KafkaProducer<String, TransactionEvent> producer = new KafkaProducer<>(KafkaConfig.producerProps());
        ClientProfile client = clients.get(RANDOM.nextInt(clients.size()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            producer.close();
        }));

        System.out.println("UnderObservationFraudProducer: generating 5 transactions for user " + client.userId());

        for (int i = 0; i < 5; i++) {
            String txId = "tx-" + UUID.randomUUID().toString().substring(0, 8);
            String accountId = client.accounts().get(RANDOM.nextInt(client.accounts().size()));
            String type = TRANSACTION_TYPES.get(RANDOM.nextInt(TRANSACTION_TYPES.size()));
            double amount = 150.0 + RANDOM.nextDouble() * 350.0;
            amount = Math.round(amount * 100.0) / 100.0;
            String deviceId = client.trustedDevices().get(RANDOM.nextInt(client.trustedDevices().size()));

            TransactionEvent tx = new TransactionEvent(
                txId, accountId, client.userId(), type, amount,
                deviceId, client.homeIp(), "ext-account-" + UUID.randomUUID().toString().substring(0, 4),
                System.currentTimeMillis()
            );

            producer.send(new ProducerRecord<>(KafkaConfig.TOPIC_TRANSACTIONS_RAW, client.userId(), tx));
            System.out.printf("TX  -> %-17s | %-7s | R$%-8s | user=%s%n", txId, type, String.format("%.2f", amount), client.userId());

            try {
                Thread.sleep(300 + RANDOM.nextInt(201));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        producer.close();
        System.out.println("UnderObservationFraudProducer finished. Check for UNDER_OBSERVATION alerts.");
    }
}
