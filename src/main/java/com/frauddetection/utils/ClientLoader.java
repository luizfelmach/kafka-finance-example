package com.frauddetection.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClientLoader {

    public static List<ClientProfile> loadClients(String filename) {
        List<ClientProfile> clients = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode root = mapper.readTree(new File(filename));
            JsonNode clientsNode = root.get("clients");
            for (JsonNode node : clientsNode) {
                String userId = node.get("user_id").asText();
                String homeIp = node.get("home_ip").asText();

                List<String> accounts = new ArrayList<>();
                for (JsonNode acc : node.get("accounts")) {
                    accounts.add(acc.asText());
                }

                List<String> devices = new ArrayList<>();
                for (JsonNode dev : node.get("trusted_devices")) {
                    devices.add(dev.asText());
                }

                double homeLatitude = node.has("home_latitude") ? node.get("home_latitude").asDouble() : 0.0;
                double homeLongitude = node.has("home_longitude") ? node.get("home_longitude").asDouble() : 0.0;

                clients.add(new ClientProfile(userId, accounts, devices, homeIp, homeLatitude, homeLongitude));
            }
        } catch (IOException e) {
            System.err.println("Failed to load clients: " + e.getMessage());
            e.printStackTrace();
        }
        return clients;
    }
}
