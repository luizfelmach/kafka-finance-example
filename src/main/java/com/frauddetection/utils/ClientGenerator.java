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
    private static final List<String> MERCHANT_CATEGORIES = List.of(
        "grocery",
        "transport",
        "electronics",
        "restaurant",
        "pharmacy",
        "gas_station"
    );

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

            List<String> categories = pickRandomCategories(2);

            Map<String, Object> client = new LinkedHashMap<>();
            client.put("user_id", userId);
            client.put("accounts", accounts);
            client.put("trusted_devices", devices);
            client.put(
                "average_transaction_amount",
                round(100 + RANDOM.nextDouble() * 600)
            );
            client.put(
                "home_ip",
                String.format(
                    "177.10.%d.%d",
                    RANDOM.nextInt(256),
                    RANDOM.nextInt(256)
                )
            );
            client.put("typical_merchant_categories", categories);
            client.put("typical_hours", pickTypicalHours());
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

    private static List<String> pickRandomCategories(int max) {
        List<String> copy = new ArrayList<>(MERCHANT_CATEGORIES);
        Collections.shuffle(copy, RANDOM);
        return copy.subList(0, 1 + RANDOM.nextInt(max));
    }

    private static String pickTypicalHours() {
        int start = 6 + RANDOM.nextInt(6);
        int end = 18 + RANDOM.nextInt(6);
        return String.format("%02d:00-%02d:00", start, end);
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
