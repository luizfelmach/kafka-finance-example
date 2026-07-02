package com.frauddetection.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.frauddetection.config.KafkaConfig;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ClientGenerator {

    private static final Random RANDOM = new Random(42);
    private static final int DEFAULT_TOTAL = 100;
    private static final int MAX_ACCOUNTS = 2;
    private static final int MAX_DEVICES = 2;

    public static void main(String[] args) throws IOException {
        int total = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_TOTAL;
        String outputFile = args.length > 1 ? args[1] : "clients.json";

        List<ClientProfile> profiles = new ArrayList<>();
        List<Map<String, Object>> clients = new ArrayList<>();

        for (int i = 0; i < total; i++) {
            String userId = String.format("u-%03d", i);

            int numAccounts = 1 + RANDOM.nextInt(MAX_ACCOUNTS);
            List<String> accounts = new ArrayList<>();
            for (int j = 0; j < numAccounts; j++) {
                accounts.add(String.format("acc-%s-%d", userId, j));
            }

            int numDevices = 1 + RANDOM.nextInt(MAX_DEVICES);
            List<String> devices = new ArrayList<>();
            for (int j = 0; j < numDevices; j++) {
                devices.add(String.format("dev-%s-%d", userId, j));
            }

            double homeLatitude = round(-15.0 + RANDOM.nextDouble() * -10.0);
            double homeLongitude = round(-40.0 + RANDOM.nextDouble() * -10.0);

            String homeIp = String.format(
                "177.10.%d.%d",
                RANDOM.nextInt(256),
                RANDOM.nextInt(256)
            );

            Map<String, Object> client = new LinkedHashMap<>();
            client.put("user_id", userId);
            client.put("accounts", accounts);
            client.put("trusted_devices", devices);
            client.put("home_ip", homeIp);
            client.put("home_latitude", homeLatitude);
            client.put("home_longitude", homeLongitude);
            clients.add(client);

            profiles.add(new ClientProfile(
                userId, accounts, devices, homeIp, homeLatitude, homeLongitude
            ));
        }

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generated_at", Instant.now().toString());
        root.put("total_clients", total);
        root.put("clients", clients);

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(new File(outputFile), root);

        System.out.println("Generated " + total + " clients -> " + outputFile);

        try (KafkaProducer<String, ClientProfile> producer = new KafkaProducer<>(KafkaConfig.producerProps())) {
            for (ClientProfile profile : profiles) {
                producer.send(new ProducerRecord<>(
                    KafkaConfig.TOPIC_CLIENTS_PROFILES,
                    profile.userId(),
                    profile
                ));
            }
            System.out.println("Published " + total + " profiles -> " + KafkaConfig.TOPIC_CLIENTS_PROFILES);
        }
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
