package com.gladys.bridge.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                    org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration.class));

    @Test
    void validatesSharedTopicDefaults() {
        context.withPropertyValues("spring.config.location=classpath:/application.yml").run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx.getEnvironment().getProperty("bridge.mqtt.telemetry-topic"))
                    .isEqualTo("gladys/devices/+/telemetry");
            assertThat(ctx.getEnvironment().getProperty("bridge.topics.telemetry"))
                    .isEqualTo("gladys.telemetry.raw.v1");
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
