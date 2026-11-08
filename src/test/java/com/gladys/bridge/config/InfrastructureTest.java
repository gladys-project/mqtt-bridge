package com.gladys.bridge.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class InfrastructureTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withPropertyValues("spring.config.location=classpath:/application-test.yml")
            .withUserConfiguration(MqttConfiguration.class, PropertiesTest.BindingConfiguration.class)
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                    org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration.class));

    @Test
    void createsDisconnectedMqttClientAndExplicitMqtt311Options() {
        context.run(ctx -> {
            assertThat(ctx).hasSingleBean(MqttAsyncClient.class).hasSingleBean(MqttConnectOptions.class);
            var client = ctx.getBean(MqttAsyncClient.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.getServerURI()).isEqualTo("tcp://127.0.0.1:1");
            assertThat(client.getClientId()).isEqualTo("mqtt-kafka-bridge");
            var options = ctx.getBean(MqttConnectOptions.class);
            assertThat(options.getMqttVersion()).isEqualTo(MqttConnectOptions.MQTT_VERSION_3_1_1);
            assertThat(options.getConnectionTimeout()).isEqualTo(5);
            assertThat(options.getKeepAliveInterval()).isEqualTo(30);
            assertThat(options.isCleanSession()).isTrue();
            assertThat(options.isAutomaticReconnect()).isFalse();
            assertThat(options.getUserName()).isNull();
            assertThat(options.getPassword()).isNull();
        });
    }

    @Test
    void validatesSharedDefaultsAndLeavesMqttDisconnected() {
        context.withPropertyValues("spring.config.location=classpath:/application.yml",
                "bridge.mqtt.username=", "bridge.mqtt.password=").run(ctx -> {
            assertThat(ctx).hasNotFailed();
            var mqtt = ctx.getBean(com.gladys.bridge.config.properties.MqttProperties.class);
            assertThat(mqtt.telemetryTopic()).isEqualTo("gladys/devices/+/telemetry");
            assertThat(mqtt.deviceEventTopic()).isEqualTo("gladys/devices/+/events");
            assertThat(mqtt.commandTopicTemplate()).isEqualTo("gladys/devices/{deviceId}/commands");
            var topics = ctx.getBean(com.gladys.bridge.config.properties.BridgeProperties.class).topics();
            assertThat(topics.telemetry()).isEqualTo("gladys.telemetry.raw.v1");
            assertThat(topics.deviceEvents()).isEqualTo("gladys.device.events.v1");
            assertThat(topics.commands()).isEqualTo("gladys.device.commands.v1");
            assertThat(ctx.getBean(MqttAsyncClient.class).isConnected()).isFalse();
        });
    }

    @Test
    void appliesEnvironmentStyleKafkaBrokerOverride() {
        context.withInitializer(ctx -> ctx.getEnvironment().getPropertySources().addFirst(
                new org.springframework.core.env.SystemEnvironmentPropertySource("systemEnvironment",
                        java.util.Map.of("SPRING_KAFKA_BOOTSTRAPSERVERS", "broker.example:19092"))))
                .run(ctx -> {
                    var config = ctx.getBean(ProducerFactory.class).getConfigurationProperties();
                    assertThat(config.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                            .isEqualTo(java.util.List.of("broker.example:19092"));
                    var consumer = ctx.getBean(ConsumerFactory.class).getConfigurationProperties();
                    assertThat(consumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
                            .isEqualTo(java.util.List.of("broker.example:19092"));
                });
    }

    @Test
    void configuresOptionalCredentials() {
        context.withPropertyValues("bridge.mqtt.username=bridge-user", "bridge.mqtt.password=test-password")
                .run(ctx -> {
                    var options = ctx.getBean(MqttConnectOptions.class);
                    assertThat(options.getUserName()).isEqualTo("bridge-user");
                    assertThat(options.getPassword()).containsExactly("test-password".toCharArray());
                });
    }

    @Test
    void usesBootKafkaFactoriesWithoutProvisioningOrListeners() {
        context.run(ctx -> {
            assertThat(ctx).hasSingleBean(KafkaTemplate.class).hasSingleBean(ProducerFactory.class)
                    .hasSingleBean(ConsumerFactory.class).hasSingleBean(KafkaAdmin.class);
            var producer = ctx.getBean(ProducerFactory.class).getConfigurationProperties();
            assertThat(producer.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
            assertThat(producer.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArraySerializer.class);
            assertThat(producer.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
            var consumer = ctx.getBean(ConsumerFactory.class).getConfigurationProperties();
            assertThat(consumer.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
            assertThat(consumer.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArrayDeserializer.class);
            assertThat(consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
            assertThat(consumer.get(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG)).isEqualTo("false");
            assertThat(ctx.getBeansOfType(org.apache.kafka.clients.admin.NewTopic.class)).isEmpty();
            var registry = ctx.getBean(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class);
            assertThat(registry.getListenerContainers()).isEmpty();
            assertThat(ctx.getBean(org.springframework.boot.kafka.autoconfigure.KafkaProperties.class).getAdmin().isAutoCreate()).isFalse();
        });
    }
}
