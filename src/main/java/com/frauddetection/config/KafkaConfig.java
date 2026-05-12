package com.frauddetection.config;

import java.util.Properties;

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
        // TODO: implementar configurações do producer
        return new Properties();
    }

    public static Properties consumerProps(String groupId) {
        // TODO: implementar configurações do consumer
        return new Properties();
    }
}
