package com.gladys.bridge.config;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MqttShutdownTest {
    @Test
    void disconnectsTheConnectedClient() throws Exception {
        var client = mock(Mqtt5AsyncClient.class);
        when(client.getState()).thenReturn(MqttClientState.CONNECTED);
        when(client.disconnect()).thenReturn(CompletableFuture.completedFuture(null));

        new MqttClientConfig().mqttShutdown(client).destroy();

        verify(client).disconnect();
    }

    @Test
    void closingAnOfflineClientDoesNotAttemptToDisconnect() throws Exception {
        var client = mock(Mqtt5AsyncClient.class);
        when(client.getState()).thenReturn(MqttClientState.DISCONNECTED);

        new MqttClientConfig().mqttShutdown(client).destroy();

        verify(client, never()).disconnect();
    }

    @Test
    void unfinishedDisconnectCannotHoldShutdownIndefinitely() {
        var client = mock(Mqtt5AsyncClient.class);
        when(client.getState()).thenReturn(MqttClientState.CONNECTED);
        when(client.disconnect()).thenReturn(new CompletableFuture<>());

        assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                assertThatCode(() -> new MqttClientConfig().mqttShutdown(client).destroy()).doesNotThrowAnyException());
    }
}
