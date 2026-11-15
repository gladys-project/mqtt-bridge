package com.gladys.bridge.config;

import java.util.concurrent.CompletableFuture;

import com.gladys.bridge.event.TelemetryReadingProcessor;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MqttStartupTest {
    @Test
    void defaultConfigurationDoesNotRegisterTheStartupRunner() {
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withPropertyValues("spring.config.location=classpath:/application-test.yml")
                .withUserConfiguration(MqttClientConfig.class)
                .run(ctx -> assertThat(ctx).hasNotFailed().doesNotHaveBean("startBridge"));
    }

    @Test
    void deployedConfigurationRegistersTheStartupRunnerWithoutConnectingDuringBeanCreation() {
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withPropertyValues("spring.config.location=classpath:/application-test.yml",
                        "spring.profiles.active=deployed")
                .withUserConfiguration(MqttClientConfig.class)
                .withBean(TelemetryReadingProcessor.class, () -> mock(TelemetryReadingProcessor.class))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed().hasBean("startBridge");
                    assertThat(ctx.getBean(Mqtt5AsyncClient.class).getState())
                            .isEqualTo(com.hivemq.client.mqtt.MqttClientState.DISCONNECTED);
                });
    }

    @Test
    void startupSubscribesOnlyAfterConnectionCompletes() throws Exception {
        var client = mock(Mqtt5AsyncClient.class);
        var processor = mock(TelemetryReadingProcessor.class);
        var connection = new CompletableFuture<Mqtt5ConnAck>();
        when(client.connect()).thenReturn(connection);
        when(processor.subscribe()).thenReturn(new CompletableFuture<Mqtt5SubAck>());

        new MqttClientConfig().startBridge(client, processor).run(new DefaultApplicationArguments());

        verify(client).connect();
        verifyNoInteractions(processor);
        connection.complete(mock(Mqtt5ConnAck.class));
        verify(processor).subscribe();
    }

    @Test
    void failedConnectionDoesNotSubscribe() throws Exception {
        var client = mock(Mqtt5AsyncClient.class);
        var processor = mock(TelemetryReadingProcessor.class);
        when(client.connect()).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Broker unavailable")));

        new MqttClientConfig().startBridge(client, processor).run(new DefaultApplicationArguments());

        verifyNoInteractions(processor);
    }
}
