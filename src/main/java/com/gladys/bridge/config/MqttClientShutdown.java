package com.gladys.bridge.config;

import java.time.Duration;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

/** Bounded blocking is confined to context shutdown, outside request/event paths. */
final class MqttClientShutdown implements DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(MqttClientShutdown.class);
    private final MqttAsyncClient client;
    private final long timeoutMillis;

    MqttClientShutdown(MqttAsyncClient client, Duration timeout) {
        this.client = client;
        this.timeoutMillis = timeout.toMillis();
    }

    @Override
    public void destroy() throws MqttException {
        try {
            if (client.isConnected()) {
                try {
                    client.disconnect(0).waitForCompletion(timeoutMillis);
                } catch (MqttException failure) {
                    log.warn("MQTT graceful shutdown failed (reason {}); forcing local disconnect", failure.getReasonCode());
                    try {
                        client.disconnectForcibly(0, timeoutMillis, false);
                    } catch (MqttException forcedFailure) {
                        log.warn("MQTT forced disconnect failed (reason {}); closing resources", forcedFailure.getReasonCode());
                    }
                }
            }
        } finally {
            client.close(true);
        }
    }
}
