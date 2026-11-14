package com.gladys.bridge.contract;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SpecificationVersion;
import io.swagger.v3.parser.OpenAPIV3Parser;
import org.yaml.snakeyaml.Yaml;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import static org.assertj.core.api.Assertions.assertThat;

class MessageContractTest {
    private static final Path CONTRACT = Path.of("src/main/resources/openapi/mqtt-inbound.yml");
    private static final Path EXAMPLE = Path.of("src/main/resources/openapi/examples/soil-moisture-reading.json");
    private final JsonMapper json = JsonMapper.builder().build();

    @Test
    void exactlyTwoOpenApiFilesDeclareTheMqttInputAndCompleteKafkaOutput() throws Exception {
        var directory = CONTRACT.getParent();
        try (var files = Files.list(directory)) {
            assertThat(files.filter(path -> path.toString().endsWith(".yml") || path.toString().endsWith(".yaml"))
                    .map(path -> path.getFileName().toString()).toList())
                    .containsExactlyInAnyOrder("mqtt-inbound.yml", "kafka-raw-telemetry.yml");
        }
        for (String name : List.of("MqttTelemetryMessage", "KafkaRawTelemetry")) {
            var options = new io.swagger.v3.parser.core.models.ParseOptions();
            options.setResolve(true);
            var parsed = new OpenAPIV3Parser().readLocation(contract(name).toAbsolutePath().toString(), null, options);
            assertThat(parsed.getMessages()).as("OpenAPI validation for %s", name).isEmpty();
            assertThat(parsed.getOpenAPI().getOpenapi()).isEqualTo("3.1.2");
            assertThat(parsed.getOpenAPI().getPaths()).isEmpty();
            assertThat(parsed.getOpenAPI().getComponents().getSchemas()).containsKey(name);
        }
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
    @ValueSource(strings = {"sensorId"})
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
        invalid.put("eventType", "");
        assertInvalid(invalid);
        invalid = example();
        invalid.put("mock", "true");
        assertInvalid(invalid);
    }

    @Test
    void rejectsInvalidPayloadAndErrorValues() throws Exception {
        var invalid = example();
        ((ObjectNode) invalid.get("payload")).put("sensorId", "");
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

    @ParameterizedTest
    @ValueSource(strings = {"KafkaRawTelemetry"})
    void kafkaOutputExamplesSatisfyTheirSchemas(String name) throws Exception {
        assertThat(validate(name, outputExample(name))).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"KafkaRawTelemetry"})
    void kafkaOutputsRequireDeviceAndEventCorrelation(String name) throws Exception {
        for (String field : List.of("deviceId", "eventId", "bootId", "sequence", "observedAt")) {
            var invalid = outputExample(name);
            invalid.remove(field);
            assertThat(validate(name, invalid)).as("%s requires %s", name, field).isNotEmpty();
        }
        var invalid = outputExample(name);
        invalid.put("deviceId", "not-a-uuid");
        assertThat(validate(name, invalid)).isNotEmpty();
    }

    @Test
    void kafkaRawContractPreservesTheWholeObservationIncludingDeviceAndSensorData() throws Exception {
        var raw = outputExample("KafkaRawTelemetry");
        assertThat(validate("KafkaRawTelemetry", raw)).isEmpty();
        assertThat(raw).isEqualTo(example());
        assertThat(raw.has("batteryLevelPct")).isTrue();
        assertThat(raw.has("errors")).isTrue();
        assertThat(raw.at("/payload/sensorId").asString()).isEqualTo("soil-001");
        assertThat(raw.at("/payload/calibrationId").asString()).isEqualTo("mock-soil-v1");
    }

    @Test
    void supportsDifferentSensorTypesAndSoilMetadataWithoutInterpretingMeasurements() throws Exception {
        var observation = example();
        observation.put("eventType", "SOIL_TEMPERATURE_READING");
        observation.putObject("payload").put("sensorId", "temperature-002")
                .put("value", 18.5).put("unit", "degC")
                .put("calibrationId", "temperature-v2");
        assertThat(validate("MqttTelemetryMessage", observation)).isEmpty();
        assertThat(validate("KafkaRawTelemetry", observation)).isEmpty();
    }

    private ObjectNode outputExample(String name) throws Exception {
        String file = "kafka-raw-telemetry.json";
        return (ObjectNode) json.readTree(Files.readString(EXAMPLE.resolveSibling(file)));
    }

    private Path contract(String name) {
        return switch (name) {
            case "KafkaRawTelemetry" -> CONTRACT.resolveSibling("kafka-raw-telemetry.yml");
            default -> CONTRACT;
        };
    }

    private Schema schema(String name) throws Exception {
        // Resolve cross-file references from an in-memory registry; fast tests never fetch schemas over the network.
        String base = "https://schemas.gladys.test/openapi/";
        var documents = new java.util.HashMap<String, String>();
        for (String model : List.of("MqttTelemetryMessage", "KafkaRawTelemetry")) {
            var path = contract(model);
            documents.put(base + path.getFileName(), json.writeValueAsString(new Yaml().load(Files.readString(path))));
        }
        return SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12, builder -> builder.schemas(documents))
                .getSchema(SchemaLocation.of(base + contract(name).getFileName() + "#/components/schemas/" + name));
    }

    private java.util.List<com.networknt.schema.Error> validate(String name, JsonNode message) throws Exception {
        return schema(name).validate(message.toString(), InputFormat.JSON,
                ctx -> ctx.executionConfig(config -> config.formatAssertionsEnabled(true)));
    }

    private ObjectNode example() throws Exception {
        return (ObjectNode) json.readTree(Files.readString(EXAMPLE));
    }

    private void assertValid(JsonNode message) throws Exception {
        assertThat(schema("MqttTelemetryMessage").validate(message.toString(), InputFormat.JSON,
                ctx -> ctx.executionConfig(config -> config.formatAssertionsEnabled(true)))).isEmpty();
    }

    private void assertInvalid(JsonNode message) throws Exception {
        assertThat(schema("MqttTelemetryMessage").validate(message.toString(), InputFormat.JSON,
                ctx -> ctx.executionConfig(config -> config.formatAssertionsEnabled(true)))).isNotEmpty();
    }
}
