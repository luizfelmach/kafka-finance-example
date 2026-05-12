package com.frauddetection.consumers;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.serialization.TransactionEventDeserializer;
import com.frauddetection.utils.ClientLoader;
import com.frauddetection.utils.ClientProfile;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class UnknownDeviceConsumer {

    public static void main(String[] args) {
        List<ClientProfile> clients = ClientLoader.loadClients("clients.json");
        Map<String, List<String>> trustedDevices = new HashMap<>();
        for (ClientProfile c : clients) {
            trustedDevices.put(c.userId(), c.trustedDevices());
        }

        Properties props = KafkaConfig.consumerProps("unknown-device-consumer");
        props.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            TransactionEventDeserializer.class.getName()
        );

        KafkaConsumer<String, TransactionEvent> consumer = new KafkaConsumer<>(
            props
        );
        consumer.subscribe(
            Collections.singletonList(KafkaConfig.TOPIC_TRANSACTIONS_RAW)
        );

        System.out.println(
            "UnknownDeviceConsumer started. Monitoring transactions..."
        );

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                System.out.println("\nShutting down UnknownDeviceConsumer...");
                consumer.close();
            })
        );

        while (true) {
            ConsumerRecords<String, TransactionEvent> records = consumer.poll(
                Duration.ofMillis(500)
            );

            for (ConsumerRecord<String, TransactionEvent> record : records) {
                TransactionEvent tx = record.value();
                if (tx == null) continue;

                String userId = tx.getUserId();
                String deviceId = tx.getDeviceId();
                List<String> trusted = trustedDevices.get(userId);

                if (trusted == null || !trusted.contains(deviceId)) {
                    System.out.printf(
                        "[ALERT] UNKNOWN_DEVICE | tx=%s | device=%s | user=%s | trusted=%s%n",
                        tx.getTransactionId(),
                        deviceId,
                        userId,
                        trusted
                    );
                }
            }
        }
    }
}
