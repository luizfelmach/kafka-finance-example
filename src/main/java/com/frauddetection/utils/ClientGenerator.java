package com.frauddetection.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

public class ClientGenerator {

    private static final Random RANDOM = new Random(42); // seed fixa para reprodutibilidade
    private static final int DEFAULT_TOTAL = 100;
    private static final int MAX_ACCOUNTS = 2;
    private static final int MAX_DEVICES = 2;

    public static void main(String[] args) throws IOException {
        int total = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_TOTAL;
        String outputFile = args.length > 1 ? args[1] : "clients.json";

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

            Map<String, Object> client = new LinkedHashMap<>();
            client.put("user_id", userId);
            client.put("accounts", accounts);
            client.put("trusted_devices", devices);
            client.put(
                "home_ip",
                String.format(
                    "177.10.%d.%d",
                    RANDOM.nextInt(256),
                    RANDOM.nextInt(256)
                )
            );
            clients.add(client);
        }

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generated_at", Instant.now().toString());
        root.put("total_clients", total);
        root.put("clients", clients);

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(new File(outputFile), root);

        System.out.println("Generated " + total + " clients -> " + outputFile);
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
