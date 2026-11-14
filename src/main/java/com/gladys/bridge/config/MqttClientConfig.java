package com.gladys.bridge.config;

import java.util.concurrent.CompletableFuture;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration(proxyBeanMethods = false)
public class MqttClientConfig {

    @Bean
    public Mqtt5AsyncClient mqtt5AsyncClient(
            @Value("${bridge.mqtt.identifier}") String identifier,
            @Value("${bridge.mqtt.host}") String host,
            @Value("${bridge.mqtt.port}") int port) {
        return Mqtt5Client.builder()
                .identifier(identifier)
                .serverHost(host)
                .serverPort(port)
                .buildAsync();
    }

    @Bean
    public Mqtt5Subscribe telemetrySubscription(
            @Value("${bridge.mqtt.telemetry-topic}") String topicFilter,
            @Value("${bridge.mqtt.qos}") int qos) {
        var mqttQos = MqttQos.fromCode(qos);
        if (mqttQos == null) {
            throw new IllegalArgumentException("MQTT QoS must be 0, 1 or 2");
        }
        return Mqtt5Subscribe.builder().topicFilter(topicFilter).qos(mqttQos).build();
    }

    /** Connect only when explicitly requested; creating the client bean remains offline. */
    public CompletableFuture<Mqtt5ConnAck> connectSubscriber(Mqtt5AsyncClient client) {
        return client.connect().whenComplete((_, failure) -> {
            if (failure != null) {
                log.error("Failed to connect to MQTT", failure);
            }
        });
    }

}
