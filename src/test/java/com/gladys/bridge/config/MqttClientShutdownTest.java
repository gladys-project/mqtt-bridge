package com.gladys.bridge.config;

import java.time.Duration;

import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class MqttClientShutdownTest {
    private final MqttAsyncClient client = mock(MqttAsyncClient.class);
    private final Duration timeout = Duration.ofMillis(250);

    @Test
    void closesAnUnusedClientWithoutConnecting() throws Exception {
        new MqttClientShutdown(client, timeout).destroy();
        verify(client).isConnected();
        verify(client).close(true);
        verifyNoMoreInteractions(client);
    }

    @Test
    void awaitsDisconnectWithConfiguredBoundThenCloses() throws Exception {
        when(client.isConnected()).thenReturn(true);
        var token = mock(IMqttToken.class);
        when(client.disconnect(0)).thenReturn(token);
        new MqttClientShutdown(client, timeout).destroy();
        var order = inOrder(client, token);
        order.verify(client).disconnect(0);
        order.verify(token).waitForCompletion(250);
        order.verify(client).close(true);
        verify(client, never()).disconnectForcibly(anyLong(), anyLong(), anyBoolean());
    }

    @Test
    void forcesDisconnectOnTimeoutAndAlwaysCloses() throws Exception {
        when(client.isConnected()).thenReturn(true);
        var token = mock(IMqttToken.class);
        when(client.disconnect(0)).thenReturn(token);
        doThrow(new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT)).when(token).waitForCompletion(250);
        doThrow(new MqttException(MqttException.REASON_CODE_CLIENT_EXCEPTION))
                .when(client).disconnectForcibly(0, 250, false);
        new MqttClientShutdown(client, timeout).destroy();
        var order = inOrder(client);
        order.verify(client).disconnectForcibly(0, 250, false);
        order.verify(client).close(true);
    }
}
