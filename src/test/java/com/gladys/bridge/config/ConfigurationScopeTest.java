package com.gladys.bridge.config;

import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ClassUtils;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.config.location=classpath:/application-test.yml")
class ConfigurationScopeTest {
    @Autowired
    ApplicationContext context;

    @Test
    void selectsHiveMqWithoutPahoAndKeepsTheClientDisconnected() throws Exception {
        var loader = getClass().getClassLoader();
        assertThat(ClassUtils.isPresent("com.hivemq.client.mqtt.MqttClient", loader)).isTrue();
        assertThat(ClassUtils.isPresent("org.eclipse.paho.client.mqttv3.MqttAsyncClient", loader)).isFalse();
        var client = context.getBean(com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient.class);
        assertThat(client.getState()).isEqualTo(com.hivemq.client.mqtt.MqttClientState.DISCONNECTED);
    }

    @Test
    void declaresNoCoreMessageProcessorsOrSources() {
        assertThat(context.getBeansOfType(Function.class).values()).allSatisfy(function ->
                assertThat(function.getClass().getPackageName()).doesNotStartWith("com.gladys.bridge"));
        assertThat(context.getBeansOfType(Supplier.class)).isEmpty();
        assertThat(context.getBeansOfType(com.gladys.bridge.mapping.MqttToKafkaMapper.class)).hasSize(1);
        assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.autodetect", Boolean.class)).isFalse();
    }
}
