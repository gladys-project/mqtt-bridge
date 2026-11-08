package com.gladys.bridge.config;

import java.util.Map;
import java.time.Duration;

import com.gladys.bridge.config.properties.BridgeProperties;
import com.gladys.bridge.config.properties.MqttProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

class PropertiesTest {
    @TestConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties({MqttProperties.class, BridgeProperties.class})
    static class BindingConfiguration {
    }

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withPropertyValues("spring.config.location=classpath:/application-test.yml")
            .withUserConfiguration(BindingConfiguration.class);

    @Test
    void bindsValidPropertiesWithoutCredentials() {
        context.run(ctx -> {
            assertThat(ctx).hasNotFailed();
            var mqtt = ctx.getBean(MqttProperties.class);
            assertThat(mqtt.brokerUri()).isEqualTo("tcp://127.0.0.1:1");
            assertThat(mqtt.qos()).isEqualTo(1);
            assertThat(mqtt.connectionTimeout()).isEqualTo(Duration.ofSeconds(5));
            assertThat(mqtt.username()).isNullOrEmpty();
            assertThat(ctx.getBean(BridgeProperties.class).topics().telemetry())
                    .isEqualTo("gladys.telemetry.raw.v1");
        });
    }

    @Test
    void bindsEnvironmentStyleOverrides() {
        context.withInitializer(ctx -> ctx.getEnvironment().getPropertySources().addFirst(
                new SystemEnvironmentPropertySource("systemEnvironment", Map.of(
                        "BRIDGE_MQTT_BROKERURI", "ssl://broker.example:8883",
                        "BRIDGE_MQTT_CLIENTID", "test-bridge",
                        "BRIDGE_MQTT_USERNAME", "test-user",
                        "BRIDGE_MQTT_PASSWORD", "test-password",
                        "BRIDGE_MQTT_CONNECTIONTIMEOUT", "7s",
                        "BRIDGE_TOPICS_TELEMETRY", "custom.telemetry.v1",
                        "BRIDGE_MQTT_QOS", "2"))))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    var mqtt = ctx.getBean(MqttProperties.class);
                    assertThat(mqtt.brokerUri()).isEqualTo("ssl://broker.example:8883");
                    assertThat(mqtt.clientId()).isEqualTo("test-bridge");
                    assertThat(mqtt.username()).isEqualTo("test-user");
                    assertThat(mqtt.password()).isEqualTo("test-password");
                    assertThat(mqtt.connectionTimeout()).isEqualTo(Duration.ofSeconds(7));
                    assertThat(ctx.getBean(BridgeProperties.class).topics().telemetry()).isEqualTo("custom.telemetry.v1");
                    assertThat(mqtt.qos()).isEqualTo(2);
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "bridge.mqtt.broker-uri=http://localhost:1883", "bridge.mqtt.broker-uri=tcp://",
            "bridge.mqtt.broker-uri=tcp://host:70000", "bridge.mqtt.broker-uri=tcp://user:secret@host:1883",
            "bridge.mqtt.broker-uri=tcp://host:1883/path", "bridge.mqtt.broker-uri=tcp://host:1883?query=yes",
            "bridge.mqtt.client-id=", "bridge.mqtt.client-id=   ", "bridge.mqtt.qos=-1", "bridge.mqtt.qos=3",
            "bridge.mqtt.connection-timeout=0s", "bridge.mqtt.connection-timeout=-1s",
            "bridge.mqtt.connection-timeout=1ms", "bridge.mqtt.shutdown-timeout=0ms",
            "bridge.mqtt.shutdown-timeout=-1s", "bridge.mqtt.keep-alive=0s",
            "bridge.mqtt.telemetry-topic=", "bridge.mqtt.telemetry-topic=gladys/foo+bar/telemetry",
            "bridge.mqtt.device-event-topic=gladys/#/events", "bridge.mqtt.device-event-topic=",
            "bridge.mqtt.command-topic-template=gladys/+/commands",
            "bridge.mqtt.command-topic-template=gladys/devices/commands",
            "bridge.mqtt.command-topic-template=gladys/{deviceId}/{deviceId}/commands",
            "bridge.mqtt.command-topic-template=gladys/prefix{deviceId}/commands",
            "bridge.mqtt.command-topic-template=gladys/{other}/commands",
            "bridge.topics.telemetry=", "bridge.topics.telemetry=bad/topic",
            "bridge.topics.device-events=.", "bridge.topics.commands=..", "bridge.topics.commands=bad topic"
    })
    void rejectsInvalidConfiguration(String property) {
        context.withPropertyValues(property).run(ctx -> {
            assertThat(ctx).hasFailed();
            assertThat(ctx.getStartupFailure()).hasStackTraceContaining("Binding validation errors");
        });
    }

    @Test
    void rejectsOverlongKafkaTopic() {
        context.withPropertyValues("bridge.topics.commands=" + "a".repeat(250))
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "1", "2"})
    void acceptsAllSupportedQos(String qos) {
        context.withPropertyValues("bridge.mqtt.qos=" + qos).run(ctx -> assertThat(ctx).hasNotFailed());
    }
}
