package com.gladys.bridge.mapping;

import com.gladys.bridge.message.MqttTelemetryMessage;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import static org.assertj.core.api.Assertions.assertThat;

class MqttToKafkaMapperTest {
    private final JsonMapper json = JsonMapper.builder().build();
    private final MqttToKafkaMapper mapper = Mappers.getMapper(MqttToKafkaMapper.class);

    @Test
    void preservesTheEntireProvidedObservationInOneKafkaMessage() throws Exception {
        var original = example();
        var result = mapper.toKafka(json.treeToValue(original, MqttTelemetryMessage.class));

        assertThat(json.readTree(json.writeValueAsBytes(result))).isEqualTo(original);
    }

    @Test
    void preservesDifferentSensorsAndExtensibleMeasurementsOnTheSameDevice() throws Exception {
        var original = example();
        original.put("eventType", "SOIL_TEMPERATURE_READING");
        var payload = original.putObject("payload").put("sensorId", "temperature-002")
                .put("value", 18.5).put("unit", "degC");
        payload.putObject("measurementMetadata").put("accuracy", 0.2).putArray("samples").add(18).add(19);

        var result = mapper.toKafka(json.treeToValue(original, MqttTelemetryMessage.class));

        assertThat(json.readTree(json.writeValueAsBytes(result))).isEqualTo(original);
        assertThat(result.getDeviceId().toString()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(result.getPayload().getSensorId()).isEqualTo("temperature-002");
    }

    @Test
    void copiesDiagnosticsPayloadAndMeasurementMapInsteadOfSharingContainers() throws Exception {
        var source = json.treeToValue(example(), MqttTelemetryMessage.class);
        var result = mapper.toKafka(source);

        assertThat(result.getErrors()).isNotSameAs(source.getErrors());
        assertThat(result.getErrors().getFirst()).isNotSameAs(source.getErrors().getFirst());
        assertThat(result.getPayload()).isNotSameAs(source.getPayload());
        assertThat(result.getPayload().getAdditionalProperties()).isNotSameAs(source.getPayload().getAdditionalProperties());
        source.getErrors().getFirst().setCode("CHANGED");
        source.getPayload().setSensorId("changed");
        source.getPayload().putAdditionalProperty("rawMoisture", 0);
        assertThat(result.getErrors().getFirst().getCode()).isEqualTo("LOW_BATTERY");
        assertThat(result.getPayload().getSensorId()).isEqualTo("soil-001");
        assertThat(result.getPayload().getAdditionalProperty("rawMoisture")).isEqualTo(2550);
    }

    @Test
    void preservesZeroValuesEmptyDiagnosticsAndLargeMeasurementNumbers() throws Exception {
        var original = example();
        original.put("batteryLevelPct", 0).put("mock", false).put("sequence", 0).put("uptimeMs", 0);
        original.putArray("errors");
        ((ObjectNode) original.get("payload")).put("rawMoisture", 4_294_967_295L);

        var result = mapper.toKafka(json.treeToValue(original, MqttTelemetryMessage.class));

        assertThat(json.readTree(json.writeValueAsBytes(result))).isEqualTo(original);
    }

    @Test
    void mapsNullToNull() {
        assertThat(mapper.toKafka(null)).isNull();
    }

    @Test
    void registersTheGeneratedMapperAsASpringBean() {
        try (var context = new AnnotationConfigApplicationContext("com.gladys.bridge.mapping")) {
            assertThat(context.getBean(MqttToKafkaMapper.class)).isNotNull();
        }
    }

    private ObjectNode example() throws Exception {
        try (var input = new ClassPathResource("openapi/examples/soil-moisture-reading.json").getInputStream()) {
            return (ObjectNode) json.readTree(input);
        }
    }
}
