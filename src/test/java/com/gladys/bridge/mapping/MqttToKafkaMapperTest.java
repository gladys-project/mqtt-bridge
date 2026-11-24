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
        var payload = original.putObject("payload").put("sensorId", "5556111b-70f6-4185-aa89-f66503bc7423")
                .put("value", 18.5).put("unit", "degC");
        payload.putObject("measurementMetadata").put("accuracy", 0.2).putArray("samples").add(18).add(19);

        var result = mapper.toKafka(json.treeToValue(original, MqttTelemetryMessage.class));

        assertThat(json.readTree(json.writeValueAsBytes(result))).isEqualTo(original);
        assertThat(result.getDeviceId().toString()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(result.getPayload().getSensorId()).isEqualTo(java.util.UUID.fromString("5556111b-70f6-4185-aa89-f66503bc7423"));
    }

    @Test
    void copiesDiagnosticsPayloadAndMeasurementMapInsteadOfSharingContainers() throws Exception {
        var source = json.treeToValue(example(), MqttTelemetryMessage.class);
        source.getPayload().putAdditionalProperty("sampleTemperatureC", 23.5);
        var result = mapper.toKafka(source);

        assertThat(result.getErrors()).isNotSameAs(source.getErrors());
        assertThat(result.getErrors().getFirst()).isNotSameAs(source.getErrors().getFirst());
        assertThat(result.getPayload()).isNotSameAs(source.getPayload());
        assertThat(result.getPayload().getAdditionalProperties()).isNotSameAs(source.getPayload().getAdditionalProperties());
        source.getErrors().getFirst().setCode("SEN_02");
        source.getPayload().setSensorId(java.util.UUID.fromString("5556111b-70f6-4185-aa89-f66503bc7423"));
        source.getPayload().setRawMoisture(0L);
        source.getPayload().putAdditionalProperty("sampleTemperatureC", 0.0);
        assertThat(result.getErrors().getFirst().getCode()).isEqualTo("BAT_01");
        assertThat(result.getPayload().getSensorId()).isEqualTo(java.util.UUID.fromString("e62e100e-4e97-42e0-8dd8-46519f0114c9"));
        assertThat(result.getPayload().getRawMoisture()).isEqualTo(2550L);
        assertThat(result.getPayload().getRawMoistureMin()).isEqualTo(2542L);
        assertThat(result.getPayload().getRawMoistureMax()).isEqualTo(2560L);
        assertThat(result.getPayload().getSampleCount()).isEqualTo(16);
        assertThat(result.getPayload().getAdditionalProperties()).containsEntry("sampleTemperatureC", 23.5);
    }

    @Test
    void preservesZeroValuesEmptyDiagnosticsAndLargeMeasurementNumbers() throws Exception {
        var original = example();
        original.put("batteryLevelPct", 0).put("sequence", 0).put("uptimeMs", 0);
        original.putArray("errors");
        ((ObjectNode) original.get("payload")).put("rawMoisture", 4_294_967_295L);

        var result = mapper.toKafka(json.treeToValue(original, MqttTelemetryMessage.class));

        assertThat(json.readTree(json.writeValueAsBytes(result))).isEqualTo(original);
    }

    @Test
    void forwardsSupplyVoltageWithoutInventingABatteryPercentage() throws Exception {
        var original = example();
        original.remove("batteryLevelPct");
        original.put("supplyVoltageMv", 4750).putArray("errors");

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

    @Test
    void preservesDistinctCriticalConditionsAcrossSuccessiveSnapshots() throws Exception {
        var observation = example();
        var errors = observation.putArray("errors");
        errors.addObject().put("code", "SEN_01").put("component", "SOIL_SENSOR_A")
                .put("severity", "CRITICAL").put("message", "Sensor read failed.");
        errors.addObject().put("code", "PMP_01").put("component", "PUMP_A")
                .put("severity", "CRITICAL").put("message", "Pump activation failed.");

        var first = mapper.toKafka(json.treeToValue(observation, MqttTelemetryMessage.class));
        assertThat(json.readTree(json.writeValueAsString(first)).get("errors")).isEqualTo(errors);

        errors.remove(0);
        var next = mapper.toKafka(json.treeToValue(observation, MqttTelemetryMessage.class));
        var nextErrors = json.readTree(json.writeValueAsString(next)).get("errors");
        assertThat(nextErrors).isEqualTo(errors);
        assertThat(nextErrors.size()).isEqualTo(1);
        assertThat(nextErrors.get(0).get("code").asString()).isEqualTo("PMP_01");
        assertThat(nextErrors.get(0).get("severity").asString()).isEqualTo("CRITICAL");
    }
}
