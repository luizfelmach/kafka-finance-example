package com.frauddetection.serialization;

import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.GeoLocation;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.streams.AccountAggregate;
import com.frauddetection.streams.LoginPair;
import com.frauddetection.streams.TakeoverState;
import com.frauddetection.utils.ClientProfile;
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

    public static Serde<LoginPair> loginPair() {
        return new JsonSerde<>(LoginPair.class);
    }

    public static Serde<BigDecimal> bigDecimal() {
        return new JsonSerde<>(BigDecimal.class);
    }

    public static Serde<AccountAggregate> accountAggregate() {
        return new JsonSerde<>(AccountAggregate.class);
    }

    public static Serde<ClientProfile> clientProfile() {
        return new JsonSerde<>(ClientProfile.class);
    }

    public static Serde<GeoLocation> geoLocation() {
        return new JsonSerde<>(GeoLocation.class);
    }

    public static Serde<TakeoverState> takeoverState() {
        return new JsonSerde<>(TakeoverState.class);
    }
}
