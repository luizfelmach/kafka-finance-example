package com.frauddetection.serialization;

import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.GeoLocation;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.streams.LastLogin;
import com.frauddetection.streams.TxHistory;
import java.math.BigDecimal;
import org.apache.kafka.common.serialization.Serde;

public class JsonSerdes {

    public static Serde<TransactionEvent> transactionEvent() {
        return new JsonSerde<>(TransactionEvent.class);
    }

    public static Serde<AuthEvent> authEvent() {
        return new JsonSerde<>(AuthEvent.class);
    }

    public static Serde<FraudAlert> fraudAlert() {
        return new JsonSerde<>(FraudAlert.class);
    }

    public static Serde<LastLogin> lastLogin() {
        return new JsonSerde<>(LastLogin.class);
    }

    public static Serde<TxHistory> txHistory() {
        return new JsonSerde<>(TxHistory.class);
    }

    public static Serde<BigDecimal> bigDecimal() {
        return new JsonSerde<>(BigDecimal.class);
    }

    public static Serde<GeoLocation> geoLocation() {
        return new JsonSerde<>(GeoLocation.class);
    }
}
