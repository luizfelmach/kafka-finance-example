package com.frauddetection.producers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.TransactionEvent;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LegitimateEventProducer {

    private static final Random RANDOM = new Random();
    private static final List<String> TRANSACTION_TYPES = List.of(
        "PIX",
        "CRED",
        "DEB"
    );
    private static final String CLIENTS_FILE = "clients.json";

    public static void main(String[] args) {
        List<ClientData> clients = loadClients(CLIENTS_FILE);
        if (clients.isEmpty()) {
            System.err.println("No clients found in " + CLIENTS_FILE);
            System.exit(1);
        }

        KafkaProducer<String, TransactionEvent> txProducer =
            new KafkaProducer<>(KafkaConfig.producerProps());
        KafkaProducer<String, AuthEvent> authProducer = new KafkaProducer<>(
            KafkaConfig.producerProps()
        );

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                System.out.println("\nShutting down producers...");
                txProducer.close();
                authProducer.close();
            })
        );

        System.out.println(
            "LegitimateEventProducer started. Generating legitimate events..."
        );

        Set<String> initializedClients = new HashSet<>();

        while (true) {
            try {
                ClientData client = clients.get(RANDOM.nextInt(clients.size()));

                if (RANDOM.nextDouble() < 0.7) {
                    if (!initializedClients.contains(client.userId)) {
                        AuthEvent auth = generateAuthEvent(client);
                        authProducer.send(
                            new ProducerRecord<>(
                                KafkaConfig.TOPIC_AUTH_EVENTS,
                                client.userId,
                                auth
                            )
                        );
                        System.out.printf(
                            "AUTH-> %-17s | %-7s | user=%s (first login)%n",
                            auth.getEventId(),
                            auth.getEventType(),
                            client.userId
                        );
                        initializedClients.add(client.userId);
                        Thread.sleep(200 + RANDOM.nextInt(301)); // 200–500ms before tx
                    }

                    TransactionEvent tx = generateTransaction(client, clients);
                    txProducer.send(
                        new ProducerRecord<>(
                            KafkaConfig.TOPIC_TRANSACTIONS_RAW,
                            client.userId,
                            tx
                        )
                    );
                    System.out.printf(
                        "TX  -> %-17s | %-7s | R$%-8s | user=%s%n",
                        tx.getTransactionId(),
                        tx.getType(),
                        String.format("%.2f", tx.getAmount()),
                        client.userId
                    );
                } else {
                    AuthEvent auth = generateAuthEvent(client);
                    authProducer.send(
                        new ProducerRecord<>(
                            KafkaConfig.TOPIC_AUTH_EVENTS,
                            client.userId,
                            auth
                        )
                    );
                    System.out.printf(
                        "AUTH-> %-17s | %-7s | user=%s%n",
                        auth.getEventId(),
                        auth.getEventType(),
                        client.userId
                    );
                }

                Thread.sleep(500 + RANDOM.nextInt(1001)); // 500–1500ms
            } catch (Exception e) {
                System.err.println("Error producing event: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static List<ClientData> loadClients(String filename) {
        List<ClientData> clients = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode root = mapper.readTree(new File(filename));
            JsonNode clientsNode = root.get("clients");
            for (JsonNode node : clientsNode) {
                String userId = node.get("user_id").asText();
                String homeIp = node.get("home_ip").asText();
                double homeLatitude = node.has("home_latitude") ? node.get("home_latitude").asDouble() : 0.0;
                double homeLongitude = node.has("home_longitude") ? node.get("home_longitude").asDouble() : 0.0;

                List<String> accounts = new ArrayList<>();
                for (JsonNode acc : node.get("accounts")) {
                    accounts.add(acc.asText());
                }

                List<String> devices = new ArrayList<>();
                for (JsonNode dev : node.get("trusted_devices")) {
                    devices.add(dev.asText());
                }

                clients.add(new ClientData(userId, accounts, devices, homeIp, homeLatitude, homeLongitude));
            }
        } catch (IOException e) {
            System.err.println("Failed to load clients: " + e.getMessage());
            e.printStackTrace();
        }
        return clients;
    }

    private static TransactionEvent generateTransaction(
        ClientData client,
        List<ClientData> allClients
    ) {
        String txId = "tx-" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = pickRandom(client.accounts);
        String deviceId = pickRandom(client.trustedDevices);
        String type = TRANSACTION_TYPES.get(
            RANDOM.nextInt(TRANSACTION_TYPES.size())
        );
        double amount = 150.0 + RANDOM.nextDouble() * 50.0; // R$ 150 – R$ 200
        amount = Math.round(amount * 100.0) / 100.0;

        String destinationAccount = pickRandomDifferentAccount(
            client,
            allClients
        );

        return new TransactionEvent(
            txId,
            accountId,
            client.userId,
            type,
            amount,
            deviceId,
            client.homeIp,
            destinationAccount,
            System.currentTimeMillis()
        );
    }

    private static AuthEvent generateAuthEvent(ClientData client) {
        String eventId = "auth-" + UUID.randomUUID().toString().substring(0, 8);
        String deviceId = pickRandom(client.trustedDevices);
        return new AuthEvent(
            eventId,
            client.userId,
            "login",
            deviceId,
            client.homeIp,
            client.homeLatitude,
            client.homeLongitude,
            System.currentTimeMillis()
        );
    }

    private static String pickRandom(List<String> list) {
        return list.get(RANDOM.nextInt(list.size()));
    }

    private static String pickRandomDifferentAccount(
        ClientData exclude,
        List<ClientData> allClients
    ) {
        ClientData other;
        do {
            other = allClients.get(RANDOM.nextInt(allClients.size()));
        } while (other == exclude);
        return pickRandom(other.accounts);
    }

    private record ClientData(
        String userId,
        List<String> accounts,
        List<String> trustedDevices,
        String homeIp,
        double homeLatitude,
        double homeLongitude
    ) {}
}
