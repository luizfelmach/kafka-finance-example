package com.frauddetection.config;

import com.frauddetection.serialization.JsonSerializer;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaConfig {

    public static final String BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092";

    public static final String TOPIC_TRANSACTIONS_RAW = "transactions.raw";
    public static final String TOPIC_AUTH_EVENTS = "auth.events";
    public static final String TOPIC_CUSTOMER_PROFILE = "customer.profile";
    public static final String TOPIC_FRAUD_ALERTS = "fraud.alerts";
    public static final String TOPIC_RISK_SCORES = "risk.scores";
    public static final String TOPIC_TRANSACTIONS_BLOCKED = "transactions.blocked";
    public static final String TOPIC_TRANSACTIONS_APPROVED = "transactions.approved";
    public static final String TOPIC_TRANSACTIONS_REVIEW = "transactions.review";

    public static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName()
        );
        props.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            JsonSerializer.class.getName()
        );
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return props;
    }

    public static Properties consumerProps(String groupId) {
        // TODO: implementar configurações do consumer
        return new Properties();
    }
}
