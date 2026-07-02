package com.frauddetection.producers;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.utils.ClientLoader;
import com.frauddetection.utils.ClientProfile;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ParallelLoginFraudProducer {

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        List<ClientProfile> clients = ClientLoader.loadClients("clients.json");
        if (clients.isEmpty()) {
            System.err.println("No clients found");
            System.exit(1);
        }

        KafkaProducer<String, AuthEvent> producer = new KafkaProducer<>(KafkaConfig.producerProps());
        ClientProfile client = clients.get(RANDOM.nextInt(clients.size()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            producer.close();
        }));

        // Login 1: São Paulo, device 0
        String authId1 = "auth-" + UUID.randomUUID().toString().substring(0, 8);
        String device1 = client.trustedDevices().get(0);
        AuthEvent loginSP = new AuthEvent(
            authId1, client.userId(), "login", device1,
            client.homeIp(), -23.5, -46.6, System.currentTimeMillis()
        );
        producer.send(new ProducerRecord<>(KafkaConfig.TOPIC_AUTH_EVENTS, client.userId(), loginSP));
        System.out.printf("AUTH-> %s | LOGIN  | SP       | user=%s device=%s%n", authId1, client.userId(), device1);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Login 2: Recife, device 1 (different device)
        String authId2 = "auth-" + UUID.randomUUID().toString().substring(0, 8);
        String device2 = client.trustedDevices().get(client.trustedDevices().size() - 1);
        AuthEvent loginRecife = new AuthEvent(
            authId2, client.userId(), "login", device2,
            client.homeIp(), -8.0, -34.9, System.currentTimeMillis()
        );
        producer.send(new ProducerRecord<>(KafkaConfig.TOPIC_AUTH_EVENTS, client.userId(), loginRecife));
        System.out.printf("AUTH-> %s | LOGIN  | Recife   | user=%s device=%s%n", authId2, client.userId(), device2);

        producer.close();
        System.out.println("ParallelLoginFraudProducer finished.");
    }
}
