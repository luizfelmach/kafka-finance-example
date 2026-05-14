package com.frauddetection.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.model.FraudAlert;
import org.apache.kafka.common.serialization.Deserializer;

public class FraudAlertDeserializer implements Deserializer<FraudAlert> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public FraudAlert deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, FraudAlert.class);
        } catch (Exception e) {
            throw new RuntimeException(
                "Error deserializing JSON to FraudAlert",
                e
            );
        }
    }
}
