package com.gladys.bridge.config;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PropertiesTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withPropertyValues("spring.config.location=classpath:/application-test.yml")
            .withUserConfiguration(MqttClientConfig.class);

    @Test
    void configuresIdentifierHostAndPortWithoutConnecting() {
        context.run(ctx -> {
            assertThat(ctx).hasNotFailed().hasSingleBean(Mqtt5AsyncClient.class);
            var config = ctx.getBean(Mqtt5AsyncClient.class).getConfig();
            assertThat(config.getClientIdentifier()).hasValueSatisfying(id ->
                    assertThat(id.toString()).isEqualTo("mqtt-kafka-bridge"));
            var address = config.getTransportConfig().getServerAddress();
            assertThat(address.getHostString()).isEqualTo("127.0.0.1");
            assertThat(address.getPort()).isEqualTo(1);
            assertThat(config.getState()).isEqualTo(MqttClientState.DISCONNECTED);
        });
    }

    @Test
    void bindsEnvironmentStyleOverrides() {
        context.withInitializer(ctx -> ctx.getEnvironment().getPropertySources().addFirst(
                new SystemEnvironmentPropertySource("systemEnvironment", Map.of(
                        "BRIDGE_MQTT_IDENTIFIER", "test-bridge",
                        "BRIDGE_MQTT_HOST", "broker.example",
                        "BRIDGE_MQTT_PORT", "1884"))))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    var config = ctx.getBean(Mqtt5AsyncClient.class).getConfig();
                    assertThat(config.getClientIdentifier()).hasValueSatisfying(id ->
                            assertThat(id.toString()).isEqualTo("test-bridge"));
                    var address = config.getTransportConfig().getServerAddress();
                    assertThat(address.getHostString()).isEqualTo("broker.example");
                    assertThat(address.getPort()).isEqualTo(1884);
                    assertThat(config.getState()).isEqualTo(MqttClientState.DISCONNECTED);
                });
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 65536})
    void rejectsPortsOutsideTheValidRange(int port) {
        context.withPropertyValues("bridge.mqtt.port=" + port)
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void connectsTheConfiguredClientOnlyWhenExplicitlyRequested() throws Exception {
        var client = mock(Mqtt5AsyncClient.class);
        var acknowledgement = mock(Mqtt5ConnAck.class);
        when(client.connect()).thenReturn(CompletableFuture.completedFuture(acknowledgement));

        var connection = new MqttClientConfig().connectSubscriber(client);

        assertThat(connection.get(1, TimeUnit.SECONDS)).isSameAs(acknowledgement);
        verify(client).connect();
    }

    @Test
    void preservesConnectionFailures() {
        var client = mock(Mqtt5AsyncClient.class);
        var failure = new IllegalStateException("Test broker unavailable");
        when(client.connect()).thenReturn(CompletableFuture.failedFuture(failure));

        var connection = new MqttClientConfig().connectSubscriber(client);

        assertThatThrownBy(() -> connection.get(1, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.ExecutionException.class)
                .hasCause(failure);
    }

    @Test
    void configuresTelemetryTopicAndQosFromYaml() {
        context.run(ctx -> {
            assertThat(ctx).hasNotFailed().hasSingleBean(Mqtt5Subscribe.class);
            var subscription = ctx.getBean(Mqtt5Subscribe.class).getSubscriptions();
            assertThat(subscription).hasSize(1);
            assertThat(subscription.getFirst().getTopicFilter().toString()).isEqualTo("gladys/devices/+/telemetry");
            assertThat(subscription.getFirst().getQos().getCode()).isEqualTo(1);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void acceptsEachSupportedSubscriptionQos(int qos) {
        context.withPropertyValues("bridge.mqtt.qos=" + qos).run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx.getBean(Mqtt5Subscribe.class).getSubscriptions().getFirst().getQos().getCode())
                    .isEqualTo(qos);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 3})
    void rejectsUnsupportedSubscriptionQos(int qos) {
        context.withPropertyValues("bridge.mqtt.qos=" + qos).run(ctx -> {
            assertThat(ctx).hasFailed();
            assertThat(ctx.getStartupFailure()).hasStackTraceContaining("MQTT QoS must be 0, 1 or 2");
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "gladys/foo+bar/telemetry", "gladys/#/telemetry"})
    void rejectsInvalidSubscriptionTopicFilters(String topic) {
        context.withPropertyValues("bridge.mqtt.telemetry-topic=" + topic)
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void appliesEnvironmentOverridesToSubscriptionTopicAndQos() {
        context.withInitializer(ctx -> ctx.getEnvironment().getPropertySources().addFirst(
                new SystemEnvironmentPropertySource("systemEnvironment", Map.of(
                        "BRIDGE_MQTT_TELEMETRYTOPIC", "custom/sensors/+/readings",
                        "BRIDGE_MQTT_QOS", "2"))))
                .run(ctx -> {
                    var subscription = ctx.getBean(Mqtt5Subscribe.class).getSubscriptions().getFirst();
                    assertThat(subscription.getTopicFilter().toString()).isEqualTo("custom/sensors/+/readings");
                    assertThat(subscription.getQos().getCode()).isEqualTo(2);
                });
    }

}
