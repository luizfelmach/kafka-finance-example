package com.frauddetection.web;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.AuthEvent;
import com.frauddetection.model.TransactionEvent;
import com.frauddetection.serialization.JsonSerializer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@CrossOrigin
@RequestMapping("/api/events")
public class EventController {

    private final KafkaProducer<String, TransactionEvent> txProducer;
    private final KafkaProducer<String, AuthEvent> authProducer;

    public EventController() {
        this.txProducer = new KafkaProducer<>(KafkaConfig.producerProps());
        this.authProducer = new KafkaProducer<>(KafkaConfig.producerProps());
    }

    @PostMapping("/transaction")
    public ResponseEntity<Map<String, String>> createTransaction(@RequestBody TransactionEvent event) {
        txProducer.send(new ProducerRecord<>(
                KafkaConfig.TOPIC_TRANSACTIONS_RAW,
                event.getUserId(),
                event));
        return ResponseEntity.ok(Map.of("status", "ok", "transactionId", event.getTransactionId()));
    }

    @PostMapping("/auth")
    public ResponseEntity<Map<String, String>> createAuth(@RequestBody AuthEvent event) {
        authProducer.send(new ProducerRecord<>(
                KafkaConfig.TOPIC_AUTH_EVENTS,
                event.getUserId(),
                event));
        return ResponseEntity.ok(Map.of("status", "ok", "eventId", event.getEventId()));
    }

    @PreDestroy
    public void shutdown() {
        txProducer.close();
        authProducer.close();
    }
}
