package com.gladys.bridge.contract;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import com.gladys.bridge.message.MqttTelemetryMessage;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.config.location=classpath:/application-test.yml")
class GeneratedMessageTest {
    private final JsonMapper json;
    private final Validator validator;

    @Autowired
    GeneratedMessageTest(JsonMapper json, Validator validator) {
        this.json = json;
        this.validator = validator;
    }

    @Test
    void mapsTheSuppliedMqttMessageToGeneratedRuntimeModelsWithoutChangingItsJson() throws Exception {
        var original = Files.readString(Path.of("src/main/resources/openapi/examples/soil-moisture-reading.json"));
        var message = json.readValue(original, MqttTelemetryMessage.class);

        assertThat(message.getEventId()).isEqualTo(UUID.fromString("6b529bda-4738-45a2-9f4f-c1d4b7a8a00a"));
        assertThat(message.getDeviceId()).isEqualTo(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertThat(message.getObservedAt()).isEqualTo(1794164428500L);
        assertThat(message.getPayload().getRawMoisture()).isEqualTo(2550L);
        assertThat(message.getPayload().getSampleCount()).isEqualTo(16);
        assertThat(message.getPayload().getRawMoistureMin()).isEqualTo(2542L);
        assertThat(message.getPayload().getRawMoistureMax()).isEqualTo(2560L);
        assertThat(message.getErrors()).hasSize(1);
        assertThat(message.getErrors().getFirst().getCode()).isEqualTo("BAT_01");
        assertThat(message.getErrors().getFirst().getSeverity().toString()).isEqualTo("ERROR");
        assertThat(json.readTree(json.writeValueAsString(message))).isEqualTo(json.readTree(original));
    }

    @Test
    void generatedValidationAcceptsTheSuppliedExample() throws Exception {
        var message = json.readValue(Files.readString(Path.of("src/main/resources/openapi/examples/soil-moisture-reading.json")),
                MqttTelemetryMessage.class);
        assertThat(validator.validate(message)).isEmpty();
    }

    @Test
    void generatedValidationCascadesIntoNestedPayloadAndErrors() throws Exception {
        var message = json.readValue(Files.readString(Path.of("src/main/resources/openapi/examples/soil-moisture-reading.json")),
                MqttTelemetryMessage.class);
        message.setBatteryLevelPct(101);
        message.getPayload().setSensorId(null);
        message.getPayload().setRawMoisture(-1L);
        message.getPayload().setSampleCount(0);
        message.getErrors().getFirst().setCode("");
        assertThat(validator.validate(message)).extracting(v -> v.getPropertyPath().toString())
                .contains("batteryLevelPct", "payload.sensorId", "payload.rawMoisture", "payload.sampleCount", "errors[0].code");
    }

    @Test
    void preservesTheWholeUnsigned32BitAdcRangeDeclaredInOpenApi() throws Exception {
        var original = json.readTree(Files.readString(Path.of("src/main/resources/openapi/examples/soil-moisture-reading.json")));
        ((tools.jackson.databind.node.ObjectNode) original.get("payload")).put("rawMoisture", 4_294_967_295L);

        var message = json.treeToValue(original, MqttTelemetryMessage.class);

        assertThat(message.getPayload().getRawMoisture()).isEqualTo(4_294_967_295L);
        assertThat(json.readTree(json.writeValueAsString(message))).isEqualTo(original);
    }
}
