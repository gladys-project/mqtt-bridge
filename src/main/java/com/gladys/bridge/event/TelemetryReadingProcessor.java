package com.gladys.bridge.event;

import java.util.concurrent.CompletableFuture;

import com.gladys.bridge.mapping.MqttToKafkaMapper;
import com.gladys.bridge.message.MqttTelemetryMessage;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Service
public class TelemetryReadingProcessor {
    private final Mqtt5AsyncClient mqttClient;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MqttToKafkaMapper mqttToKafkaMapper;
    private final Mqtt5Subscribe subscription;
    private final String topic;

    public TelemetryReadingProcessor(Mqtt5AsyncClient mqttClient, KafkaTemplate<String, byte[]> kafkaTemplate,
            ObjectMapper objectMapper, MqttToKafkaMapper mqttToKafkaMapper, Mqtt5Subscribe subscription,
            @Value("${bridge.topics.telemetry}") String topic) {
        this.mqttClient = mqttClient;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.mqttToKafkaMapper = mqttToKafkaMapper;
        this.subscription = subscription;
        this.topic = topic;
    }

    /** Invoke after connection completes; bean creation performs no broker operations. */
    public CompletableFuture<Mqtt5SubAck> subscribe() {
        return mqttClient.subscribe(subscription, this::processAndSendMessage)
                .whenComplete((ack, failure) -> {
                    if (failure != null) {
                        log.error("Failed to subscribe to MQTT telemetry", failure);
                    }
                });
    }

    private void processAndSendMessage(Mqtt5Publish message) {
        try {
            var reading = objectMapper.readValue(message.getPayloadAsBytes(), MqttTelemetryMessage.class);
            var outbound = mqttToKafkaMapper.toKafka(reading);
            kafkaTemplate.send(topic, outbound.getDeviceId().toString(), objectMapper.writeValueAsBytes(outbound))
                    .whenComplete((result, failure) -> {
                        if (failure != null) {
                            log.error("Failed to publish telemetry to Kafka", failure);
                        }
                    });
        } catch (RuntimeException failure) {
            log.error("Failed to process MQTT telemetry", failure);
        }
    }
}
