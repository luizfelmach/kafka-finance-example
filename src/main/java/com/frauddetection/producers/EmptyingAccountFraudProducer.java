package com.frauddetection.producers;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.utils.ClientLoader;
import com.frauddetection.utils.ClientProfile;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EmptyingAccountFraudProducer {

    private static final Random RANDOM = new Random();
    private static final List<String> TRANSACTION_TYPES = List.of("PIX", "CRED", "DEB");
    private static final int BURST_COUNT = 4;
    private static final double HIGH_VALUE_AMOUNT = 8500.0;

    public static void main(String[] args) {
        List<ClientProfile> clients = ClientLoader.loadClients("clients.json");
        if (clients.isEmpty()) {
            System.err.println("No clients found");
            System.exit(1);
        }

        KafkaProducer<String, TransactionEvent> producer = new KafkaProducer<>(
                KafkaConfig.producerProps());

        ClientProfile client = clients.get(RANDOM.nextInt(clients.size()));
        String accountId = pickRandom(client.accounts());
        String deviceId = pickRandom(client.trustedDevices());

        System.out.println(
                "EmptyingAccountFraudProducer started for user=" + client.userId());

        for (int i = 0; i < BURST_COUNT; i++) {
            String txId = "tx-" + UUID.randomUUID().toString().substring(0, 8);
            String type = TRANSACTION_TYPES.get(RANDOM.nextInt(TRANSACTION_TYPES.size()));
            double amount = HIGH_VALUE_AMOUNT + RANDOM.nextDouble() * 500.0; // R$ 1500 - R$ 2000
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
                    destinationAccount);

            producer.send(
                    new ProducerRecord<>(
                            KafkaConfig.TOPIC_TRANSACTIONS_RAW,
                            client.userId(),
                            tx));
            System.out.printf(
                    "TX  -> %-17s | %-7s | R$%-8s | user=%s%n",
                    tx.getTransactionId(),
                    tx.getType(),
                    String.format("%.2f", tx.getAmount()),
                    client.userId());

            try {
                Thread.sleep(200); // 200ms interval
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        producer.close();
        System.out.println("EmptyingAccountFraudProducer finished.");
    }

    private static String pickRandom(List<String> list) {
        return list.get(RANDOM.nextInt(list.size()));
    }

    private static String pickRandomDifferentAccount(
            ClientProfile exclude,
            List<ClientProfile> allClients) {
        ClientProfile other;
        do {
            other = allClients.get(RANDOM.nextInt(allClients.size()));
        } while (other == exclude);
        return pickRandom(other.accounts());
    }
}
