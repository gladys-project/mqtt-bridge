package com.gladys.bridge;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;

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

    @DynamicPropertySource
    static void brokers(DynamicPropertyRegistry properties) {
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        // Only test configuration declares a NewTopic and enables explicit provisioning.
        properties.add("spring.kafka.admin.auto-create", () -> true);
        properties.add("spring.kafka.admin.fail-fast", () -> true);
        properties.add("spring.kafka.admin.operation-timeout", () -> "10s");
    }

    private final KafkaTemplate<String, byte[]> kafka;
    private final ConsumerFactory<String, byte[]> consumers;
    private final String topic;

    @Autowired
    BrokerConnectivityIT(KafkaTemplate<String, byte[]> kafka, ConsumerFactory<String, byte[]> consumers,
                         @Value("${bridge.topics.telemetry}") String topic) {
        this.kafka = kafka;
        this.consumers = consumers;
        this.topic = topic;
    }

    @Test
    @Timeout(45)
    void configuredKafkaProducerPreservesDeviceKeyAndPayload() throws Exception {
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

    @TestConfiguration(proxyBeanMethods = false)
    static class TestTopics {
        @Bean
        NewTopic connectivityTopic(@Value("${bridge.topics.telemetry}") String topic) {
            return TopicBuilder.name(topic).partitions(1).replicas(1).build();
        }
    }
}
