package com.frauddetection.web;

import com.frauddetection.config.KafkaConfig;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.serialization.JsonSerdes;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@CrossOrigin
@RequestMapping("/api/alerts")
public class AlertSSEController {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void startConsumer() {
        Thread t = new Thread(() -> {
            var props = KafkaConfig.consumerProps("fraud-alert-sse");
            try (var consumer = new KafkaConsumer<>(props,
                    new StringDeserializer(), JsonSerdes.fraudAlert().deserializer())) {
                consumer.subscribe(List.of(KafkaConfig.TOPIC_FRAUD_EVENTS));
                while (true) {
                    ConsumerRecords<String, FraudAlert> records = consumer.poll(Duration.ofMillis(500));
                    records.forEach(record -> {
                        for (SseEmitter emitter : emitters) {
                            try {
                                emitter.send(SseEmitter.event()
                                        .name("alert")
                                        .data(record.value(), MediaType.APPLICATION_JSON));
                            } catch (Exception e) {
                                emitters.remove(emitter);
                            }
                        }
                    });
                }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        return emitter;
    }
}
