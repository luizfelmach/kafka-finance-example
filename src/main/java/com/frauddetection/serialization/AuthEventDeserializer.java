package com.frauddetection.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.model.AuthEvent;
import org.apache.kafka.common.serialization.Deserializer;

public class AuthEventDeserializer implements Deserializer<AuthEvent> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public AuthEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, AuthEvent.class);
        } catch (Exception e) {
            throw new RuntimeException(
                "Error deserializing JSON to AuthEvent",
                e
            );
        }
    }
}
