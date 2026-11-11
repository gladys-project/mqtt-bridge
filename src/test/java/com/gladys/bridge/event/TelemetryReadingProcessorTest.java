package com.gladys.bridge.event;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.gladys.bridge.mapping.MqttToKafkaMapper;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(OutputCaptureExtension.class)
class TelemetryReadingProcessorTest {
    private final Mqtt5AsyncClient mqtt = mock(Mqtt5AsyncClient.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
    private final JsonMapper json = JsonMapper.builder().build();
    private final Mqtt5Subscribe subscription = Mqtt5Subscribe.builder()
            .topicFilter("custom/devices/+/readings").qos(MqttQos.AT_LEAST_ONCE).build();
    private final TelemetryReadingProcessor processor = new TelemetryReadingProcessor(mqtt, kafka, json,
            Mappers.getMapper(MqttToKafkaMapper.class), subscription, "custom.raw.telemetry");

    @Test
    void subscribesExplicitlyAndCompletesOnlyAfterSuback() throws Exception {
        verifyNoInteractions(mqtt, kafka);
        var pending = new CompletableFuture<Mqtt5SubAck>();
        when(mqtt.subscribe(eq(subscription), any())).thenReturn(pending);

        var result = processor.subscribe();

        assertThat(result).isNotDone();
        var ack = mock(Mqtt5SubAck.class);
        pending.complete(ack);
        assertThat(result.get(1, TimeUnit.SECONDS)).isSameAs(ack);
        verify(mqtt).subscribe(eq(subscription), any());
    }

    @Test
    void forwardsCompleteJsonToConfiguredKafkaTopicUsingDeviceIdAsKey() throws Exception {
        var callback = callback();
        byte[] original;
        try (var input = new ClassPathResource("openapi/examples/soil-moisture-reading.json").getInputStream()) {
            original = input.readAllBytes();
        }
        when(kafka.send(eq("custom.raw.telemetry"), any(String.class), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        callback.accept(Mqtt5Publish.builder().topic("custom/devices/test/readings").payload(original).build());

        var payload = ArgumentCaptor.forClass(byte[].class);
        verify(kafka).send(eq("custom.raw.telemetry"), eq("550e8400-e29b-41d4-a716-446655440000"), payload.capture());
        assertThat(json.readTree(payload.getValue())).isEqualTo(json.readTree(original));
    }

    @Test
    void rejectsMalformedJsonWithoutSendingToKafka(CapturedOutput output) {
        callback().accept(Mqtt5Publish.builder().topic("custom/devices/test/readings")
                .payload("not json".getBytes(java.nio.charset.StandardCharsets.UTF_8)).build());

        verifyNoInteractions(kafka);
        assertThat(output).contains("Failed to process MQTT telemetry");
    }

    @Test
    void preservesSubscriptionFailure() {
        var failure = new IllegalStateException("Subscription rejected");
        when(mqtt.subscribe(eq(subscription), any())).thenReturn(CompletableFuture.failedFuture(failure));

        var result = processor.subscribe();

        assertThatThrownBy(() -> result.get(1, TimeUnit.SECONDS)).hasCause(failure);
    }

    @Test
    void observesAsynchronousKafkaSendFailure(CapturedOutput output) throws Exception {
        var callback = callback();
        byte[] original;
        try (var input = new ClassPathResource("openapi/examples/soil-moisture-reading.json").getInputStream()) {
            original = input.readAllBytes();
        }
        when(kafka.send(any(String.class), any(String.class), any(byte[].class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));

        callback.accept(Mqtt5Publish.builder().topic("custom/devices/test/readings").payload(original).build());

        assertThat(output).contains("Failed to publish telemetry to Kafka").contains("Kafka unavailable");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Consumer<Mqtt5Publish> callback() {
        when(mqtt.subscribe(eq(subscription), any())).thenReturn(CompletableFuture.completedFuture(mock(Mqtt5SubAck.class)));
        processor.subscribe();
        ArgumentCaptor<Consumer<Mqtt5Publish>> callback = ArgumentCaptor.forClass((Class) Consumer.class);
        verify(mqtt).subscribe(eq(subscription), callback.capture());
        return callback.getValue();
    }
}
