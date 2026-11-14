package com.gladys.bridge;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.gladys.bridge.config.MqttClientConfig;
import com.gladys.bridge.event.TelemetryReadingProcessor;
import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.config.location=classpath:/application-test.yml",
        "bridge.mqtt.telemetry-topic=integration/sensors/+/readings",
        "bridge.mqtt.qos=2",
        "bridge.topics.telemetry=integration.telemetry.raw",
        "spring.kafka.admin.auto-create=true",
        "spring.kafka.admin.fail-fast=true",
        "spring.kafka.admin.operation-timeout=10s"
})
@Import(MqttKafkaFlowIT.TestTopic.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class MqttKafkaFlowIT {
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .waitingFor(Wait.forLogMessage(".*Kafka Server started.*\\n", 1))
            .withStartupTimeout(Duration.ofSeconds(90));

    @Container
    static final GenericContainer<?> MQTT = new GenericContainer<>("eclipse-mosquitto:2.0.22")
            .withExposedPorts(1883)
            .withCopyFileToContainer(MountableFile.forHostPath("infra/mosquitto/mosquitto.conf"),
                    "/mosquitto/config/mosquitto.conf")
            .waitingFor(new WaitAllStrategy().withStrategy(Wait.forListeningPort())
                    .withStrategy(Wait.forLogMessage(".*mosquitto version .* running.*\\n", 1)))
            .withStartupTimeout(Duration.ofSeconds(30));

    @DynamicPropertySource
    static void brokers(DynamicPropertyRegistry properties) {
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        properties.add("bridge.mqtt.host", MQTT::getHost);
        properties.add("bridge.mqtt.port", () -> MQTT.getMappedPort(1883));
    }

    @Autowired Mqtt5AsyncClient mqtt;
    @Autowired MqttClientConfig mqttConfig;
    @Autowired TelemetryReadingProcessor processor;
    @Autowired Mqtt5Subscribe subscription;
    @Autowired ConsumerFactory<String, byte[]> consumers;
    @Autowired JsonMapper json;
    @Value("${bridge.topics.telemetry}") String kafkaTopic;

    @Test
    @Timeout(60)
    void configuredTopicsAndQosCarryTheCompleteObservationToKafka() throws Exception {
        assertThat(mqtt.getState()).isEqualTo(MqttClientState.DISCONNECTED);
        assertThat(subscription.getSubscriptions().getFirst().getTopicFilter().toString())
                .isEqualTo("integration/sensors/+/readings");
        var consumer = consumers.createConsumer("mqtt-qos-it", "mqtt-qos-it", "-1");
        try {
            var partition = new TopicPartition(kafkaTopic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            mqttConfig.connectSubscriber(mqtt).get(10, TimeUnit.SECONDS);
            var suback = processor.subscribe().get(10, TimeUnit.SECONDS);
            assertThat(suback.getReasonCodes()).containsExactly(Mqtt5SubAckReasonCode.GRANTED_QOS_2);

            byte[] original;
            try (var input = new ClassPathResource("openapi/examples/soil-moisture-reading.json").getInputStream()) {
                original = input.readAllBytes();
            }
            // Publish only after the configured subscription has received SUBACK.
            mqtt.publish(Mqtt5Publish.builder().topic("integration/sensors/soil-001/readings")
                    .qos(MqttQos.EXACTLY_ONCE).payload(original).build()).get(10, TimeUnit.SECONDS);
            var records = new ArrayList<ConsumerRecord<String, byte[]>>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (records.isEmpty() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().topic()).isEqualTo("integration.telemetry.raw");
            assertThat(records.getFirst().key()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
            assertThat(json.readTree(records.getFirst().value())).isEqualTo(json.readTree(original));
        } finally {
            consumer.close(Duration.ofSeconds(3));
            if (mqtt.getState().isConnected()) {
                mqtt.disconnect().get(3, TimeUnit.SECONDS);
            }
        }
        assertThat(mqtt.getState()).isEqualTo(MqttClientState.DISCONNECTED);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestTopic {
        @Bean
        NewTopic rawTelemetryTopic(@Value("${bridge.topics.telemetry}") String topic) {
            return TopicBuilder.name(topic).partitions(1).replicas(1).build();
        }
    }
}
