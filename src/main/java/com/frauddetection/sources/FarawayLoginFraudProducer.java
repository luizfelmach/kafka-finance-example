package com.frauddetection.sources;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.utils.ClientLoader;
import com.frauddetection.utils.ClientProfile;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FarawayLoginFraudProducer {

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

        // Login 1: São Paulo
        String authId1 = "auth-" + UUID.randomUUID().toString().substring(0, 8);
        String device1 = client.trustedDevices().get(0);
        AuthEvent loginSP = new AuthEvent(
            authId1, client.userId(), "login", device1,
            client.homeIp(), -23.5, -46.6, System.currentTimeMillis()
        );
        producer.send(new ProducerRecord<>(KafkaConfig.TOPIC_AUTH_EVENTS, client.userId(), loginSP));
        System.out.printf("AUTH-> %s | LOGIN  | SP (-23.5, -46.6)     | user=%s%n", authId1, client.userId());

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Login 2: Tokyo (~18,500 km from SP in 500ms)
        String authId2 = "auth-" + UUID.randomUUID().toString().substring(0, 8);
        AuthEvent loginTokyo = new AuthEvent(
            authId2, client.userId(), "login", device1,
            client.homeIp(), 35.7, 139.7, System.currentTimeMillis()
        );
        producer.send(new ProducerRecord<>(KafkaConfig.TOPIC_AUTH_EVENTS, client.userId(), loginTokyo));
        System.out.printf("AUTH-> %s | LOGIN  | Tokyo (35.7, 139.7)   | user=%s%n", authId2, client.userId());

        producer.close();
        System.out.println("FarawayLoginFraudProducer finished.");
    }
}
