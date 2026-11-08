package com.gladys.bridge;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.gladys.bridge.config.MqttConfiguration;
import com.gladys.bridge.config.properties.BridgeProperties;
import com.gladys.bridge.config.properties.MqttProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.config.location=classpath:/application-test.yml")
@Import(BrokerConnectivityIT.TestTopics.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class BrokerConnectivityIT {
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
        // Only test configuration declares a NewTopic and enables explicit provisioning.
        properties.add("spring.kafka.admin.auto-create", () -> true);
        properties.add("spring.kafka.admin.fail-fast", () -> true);
        properties.add("spring.kafka.admin.operation-timeout", () -> "10s");
        properties.add("bridge.mqtt.broker-uri", () -> "tcp://" + MQTT.getHost() + ":" + MQTT.getMappedPort(1883));
    }

    private final KafkaTemplate<String, byte[]> kafka;
    private final ConsumerFactory<String, byte[]> consumers;
    private final BridgeProperties bridge;
    private final MqttProperties mqttProperties;

    @Autowired
    BrokerConnectivityIT(KafkaTemplate<String, byte[]> kafka, ConsumerFactory<String, byte[]> consumers,
                         BridgeProperties bridge, MqttProperties mqttProperties) {
        this.kafka = kafka;
        this.consumers = consumers;
        this.bridge = bridge;
        this.mqttProperties = mqttProperties;
    }

    @Test
    @Timeout(45)
    void configuredKafkaProducerPreservesDeviceKeyAndPayload() throws Exception {
        String topic = bridge.topics().telemetry();
        byte[] payload = Files.readAllBytes(Path.of("src/main/resources/openapi/examples/soil-moisture-reading.json"));
        var sent = kafka.send(topic, "550e8400-e29b-41d4-a716-446655440000", payload).get(15, TimeUnit.SECONDS);
        assertThat(sent.getRecordMetadata().topic()).isEqualTo(topic);
        var consumer = consumers.createConsumer("bridge-it", "bridge-it", "-1");
        try {
            var partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var received = new ArrayList<ConsumerRecord<String, byte[]>>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (received.isEmpty() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
            }
            assertThat(received).hasSize(1);
            assertThat(received.getFirst().key()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
            assertThat(received.getFirst().value()).containsExactly(payload);
        } finally {
            consumer.close(Duration.ofSeconds(3));
        }
    }

    @Test
    @Timeout(45)
    void configuredMqttClientConnectsSubscribesAndReceivesAfterSuback() throws Exception {
        var closedClient = new java.util.concurrent.atomic.AtomicReference<MqttAsyncClient>();
        var shutdownStarted = new java.util.concurrent.atomic.AtomicLong();
        // Own this context so cleanup can be asserted after closure without disrupting Spring's test context cache.
        new ApplicationContextRunner().withUserConfiguration(MqttConfiguration.class)
                .withBean(MqttProperties.class, () -> mqttProperties)
                .run(ctx -> {
                    var mqtt = ctx.getBean(MqttAsyncClient.class);
                    closedClient.set(mqtt);
                    assertThat(mqtt.isConnected()).isFalse();
                    var received = new CompletableFuture<byte[]>();
                    mqtt.setCallback(new MqttCallback() {
                        @Override
                        public void connectionLost(Throwable cause) {
                            received.completeExceptionally(cause != null ? cause : new IllegalStateException("MQTT connection lost"));
                        }

                        @Override
                        public void messageArrived(String topic, MqttMessage message) {
                            if (topic.equals("gladys/devices/550e8400-e29b-41d4-a716-446655440000/telemetry")) {
                                received.complete(message.getPayload().clone());
                            }
                        }

                        @Override
                        public void deliveryComplete(IMqttDeliveryToken token) {
                        }
                    });
                    mqtt.connect(ctx.getBean(MqttConnectOptions.class)).waitForCompletion(10_000);
                    var subscribed = mqtt.subscribe(mqttProperties.telemetryTopic(), mqttProperties.qos());
                    subscribed.waitForCompletion(10_000);
                    assertThat(subscribed.getGrantedQos()).containsExactly(mqttProperties.qos());
                    byte[] payload = Files.readAllBytes(Path.of("src/main/resources/openapi/examples/soil-moisture-reading.json"));
                    mqtt.publish("gladys/devices/550e8400-e29b-41d4-a716-446655440000/telemetry", payload, mqttProperties.qos(), false)
                            .waitForCompletion(10_000);
                    assertThat(received.get(10, TimeUnit.SECONDS)).containsExactly(payload);
                    shutdownStarted.set(System.nanoTime());
                });
        assertThat(Duration.ofNanos(System.nanoTime() - shutdownStarted.get())).isLessThan(Duration.ofSeconds(10));
        assertThat(closedClient.get().isConnected()).isFalse();
        assertThatThrownBy(closedClient.get()::connect).isInstanceOfSatisfying(MqttException.class,
                failure -> assertThat(failure.getReasonCode()).isEqualTo(MqttException.REASON_CODE_CLIENT_CLOSED));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestTopics {
        @Bean
        NewTopic connectivityTopic(BridgeProperties properties) {
            return TopicBuilder.name(properties.topics().telemetry()).partitions(1).replicas(1).build();
        }
    }
}
