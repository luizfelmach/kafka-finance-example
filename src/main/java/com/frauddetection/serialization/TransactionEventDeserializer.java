package com.frauddetection.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.model.TransactionEvent;
import org.apache.kafka.common.serialization.Deserializer;

public class TransactionEventDeserializer
    implements Deserializer<TransactionEvent>
{

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TransactionEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, TransactionEvent.class);
        } catch (Exception e) {
            throw new RuntimeException(
                "Error deserializing JSON to TransactionEvent",
                e
            );
        }
    }
}
