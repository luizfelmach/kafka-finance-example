package com.frauddetection.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {

    private final JsonSerializer<T> serializer = new JsonSerializer<>();
    private final JsonDeserializer<T> deserializer;

    public JsonSerde() {
        this.deserializer = new JsonDeserializer<>();
    }

    public JsonSerde(Class<T> type) {
        this.deserializer = new JsonDeserializer<>(type);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
