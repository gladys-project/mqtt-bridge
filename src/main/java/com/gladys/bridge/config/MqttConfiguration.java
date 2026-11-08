package com.gladys.bridge.config;

import com.gladys.bridge.config.properties.MqttProperties;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MqttConfiguration {
    @Bean(destroyMethod = "")
    MqttAsyncClient mqttAsyncClient(MqttProperties properties) throws MqttException {
        return new MqttAsyncClient(properties.brokerUri(), properties.clientId(), new MemoryPersistence());
    }

    @Bean
    MqttClientShutdown mqttClientShutdown(MqttAsyncClient client, MqttProperties properties) {
        return new MqttClientShutdown(client, properties.shutdownTimeout());
    }

    @Bean
    MqttConnectOptions mqttConnectOptions(MqttProperties properties) {
        var options = new MqttConnectOptions();
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setCleanSession(true);
        options.setAutomaticReconnect(false);
        options.setConnectionTimeout(Math.toIntExact(properties.connectionTimeout().toSeconds()));
        options.setKeepAliveInterval(Math.toIntExact(properties.keepAlive().toSeconds()));
        if (properties.username() != null && !properties.username().isEmpty()) {
            options.setUserName(properties.username());
        }
        if (properties.password() != null && !properties.password().isEmpty()) {
            options.setPassword(properties.password().toCharArray());
        }
        return options;
    }
}
