package com.gladys.bridge.contract;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import io.swagger.v3.parser.OpenAPIV3Parser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yaml.snakeyaml.Yaml;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import static org.assertj.core.api.Assertions.assertThat;

class MessageContractTest {
    private static final Path CONTRACT = Path.of("src/main/resources/openapi/messages.yml");
    private static final Path EXAMPLE = Path.of("src/main/resources/openapi/examples/soil-moisture-reading.json");
    private final JsonMapper json = JsonMapper.builder().build();

    @Test
    void openApiDocumentParsesWithoutErrorsAndDeclaresOnlySchemas() throws Exception {
        assertThat(CONTRACT).isRegularFile();
        var parsed = new OpenAPIV3Parser().readContents(Files.readString(CONTRACT), null, null);
        assertThat(parsed.getMessages()).isEmpty();
        assertThat(parsed.getOpenAPI().getOpenapi()).isEqualTo("3.1.2");
        assertThat(parsed.getOpenAPI().getPaths()).isEmpty();
        assertThat(parsed.getOpenAPI().getComponents().getSchemas())
                .containsKeys("SoilMoistureReading", "SoilMoisturePayload", "DeviceError");
    }

    @Test
    void suppliedMqttExampleSatisfiesItsSchema() throws Exception {
        assertValid(example());
    }

    @ParameterizedTest
    @ValueSource(strings = {"schemaVersion", "eventId", "eventType", "deviceId", "bootId", "sequence", "uptimeMs",
            "observedAt", "clockSource", "mock", "batteryLevelPct", "errors", "payload"})
    void rejectsMissingEnvelopeFields(String field) throws Exception {
        var invalid = example();
        invalid.remove(field);
        assertInvalid(invalid);
    }

    @ParameterizedTest
    @ValueSource(strings = {"plantId", "sensorId", "rawMoisture", "unit", "adcResolutionBits", "calibrationId", "quality"})
    void rejectsMissingReadingFields(String field) throws Exception {
        var invalid = example();
        ((ObjectNode) invalid.get("payload")).remove(field);
        assertInvalid(invalid);
    }

    @Test
    void rejectsInvalidEnvelopeValues() throws Exception {
        for (String field : List.of("eventId", "deviceId", "bootId")) {
            var invalid = example();
            invalid.put(field, "not-a-uuid");
            assertInvalid(invalid);
        }
        for (String field : List.of("sequence", "uptimeMs", "observedAt", "batteryLevelPct")) {
            var invalid = example();
            invalid.put(field, -1);
            assertInvalid(invalid);
        }
        var invalid = example();
        invalid.put("batteryLevelPct", 101);
        assertInvalid(invalid);
        invalid = example();
        invalid.put("schemaVersion", 2);
        assertInvalid(invalid);
        invalid = example();
        invalid.put("eventType", "PUMP_COMMAND");
        assertInvalid(invalid);
        invalid = example();
        invalid.put("mock", "true");
        assertInvalid(invalid);
    }

    @Test
    void rejectsInvalidPayloadAndErrorValues() throws Exception {
        var invalid = example();
        ((ObjectNode) invalid.get("payload")).put("rawMoisture", -1);
        assertInvalid(invalid);
        invalid = example();
        ((ObjectNode) invalid.get("payload")).put("adcResolutionBits", 0);
        assertInvalid(invalid);
        invalid = example();
        ((ObjectNode) invalid.get("payload")).put("unit", "percent");
        assertInvalid(invalid);
        invalid = example();
        ((ObjectNode) invalid.get("errors").get(0)).remove("code");
        assertInvalid(invalid);
        invalid = example();
        ((ObjectNode) invalid.get("errors").get(0)).put("severity", "UNRECOGNIZED");
        assertInvalid(invalid);
    }

    @Test
    void acceptsAnEmptyErrorsArrayAndRealDeviceReadings() throws Exception {
        var valid = example();
        valid.put("mock", false);
        valid.putArray("errors");
        assertValid(valid);
    }

    private ObjectNode example() throws Exception {
        return (ObjectNode) json.readTree(Files.readString(EXAMPLE));
    }

    private Schema schema() throws Exception {
        // Retain the OpenAPI component location so internal references resolve unchanged.
        ObjectNode document = json.valueToTree(new Yaml().load(Files.readString(CONTRACT)));
        document.put("$ref", "#/components/schemas/SoilMoistureReading");
        return SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                .getSchema(json.writeValueAsString(document), InputFormat.JSON);
    }

    private void assertValid(JsonNode message) throws Exception {
        assertThat(schema().validate(message.toString(), InputFormat.JSON,
                ctx -> ctx.executionConfig(config -> config.formatAssertionsEnabled(true)))).isEmpty();
    }

    private void assertInvalid(JsonNode message) throws Exception {
        assertThat(schema().validate(message.toString(), InputFormat.JSON,
                ctx -> ctx.executionConfig(config -> config.formatAssertionsEnabled(true)))).isNotEmpty();
    }
}
