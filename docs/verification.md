# Foundation verification — 2026-11-08

Executed with OpenJDK 25.0.4.1, the Maven Wrapper's Maven 3.9.12, and the system Docker daemon. Commands requiring Maven cache writes, network access, Docker or a local HTTP port ran outside the workspace sandbox. The selected Docker Desktop daemon was unavailable; Testcontainers found the working system Unix socket automatically.

## Observed red/green steps

| Increment | Observed red result, before its implementation | Observed green result |
| --- | --- | --- |
| Bootstrap | `ApplicationTest` failed because no `@SpringBootConfiguration` existed. | Entry point added; HTTP health test passed. |
| Typed properties | `PropertiesTest` failed to compile because `MqttProperties` and `BridgeProperties` did not exist. | 36 binding/validation cases passed after adding records and constraints. The environment-source fixture needed Spring's system-environment source name for relaxed binding. |
| MQTT infrastructure | `InfrastructureTest` failed because MQTT client/options beans were missing. | Infrastructure/options tests passed with disconnected Paho client and explicit 3.1.1 options. The final suite has 5 infrastructure tests. |
| Shutdown | `MqttClientShutdownTest` failed to compile because the shutdown component was missing. | 3 tests passed for unused-client closure, bounded graceful disconnect, and timeout/forced-disconnect failure with guaranteed closure. |
| OpenAPI contracts | 25 contract tests ran: the schema/example were absent, producing 1 failure and 24 file errors. | All 25 passed after adding the supplied MQTT example and OpenAPI schemas. |
| Real brokers | First container run timed out waiting for a Mosquitto startup log suppressed by the local log configuration. | Enabled informational startup logging; containers became ready. Both connectivity tests then passed after correcting the test's context ownership for shutdown verification. |

Infrastructure iterations also exposed test harness issues: one HTTP assertion assumed Actuator returned only `status`, and manually closing Spring's cached test context caused its later lifecycle callback to fail. Assertions now allow standard health-group fields; the MQTT cleanup check owns its context. Fast context runners explicitly import their configuration so test-only topic provisioning cannot leak into them. These corrections are test/configuration work, not message processing.

No successful result is inferred from a blocked sandbox command. Earlier sandbox runs failed on a read-only Maven cache or prohibited local sockets; the relevant checks were rerun with access.

## Final checks

`./mvnw -B -ntp verify` completed successfully:

- Surefire: 72 tests, 0 failures, 0 errors, 0 skipped.
- Failsafe: 2 tests, 0 failures, 0 errors, 0 skipped.
- Real images: `apache/kafka:4.1.0` and `eclipse-mosquitto:2.0.22`.
- Kafka: configured producer and test-only consumer preserved the device ID key and exact supplied JSON payload.
- MQTT: configured client connected using 3.1.1, completed subscription acknowledgement before publish, received the exact payload, and disconnected/closed through configured context cleanup.

A final standalone `./mvnw -B -ntp test` passed with 72 tests, 0 failures, 0 errors and 0 skipped after stopping the validation brokers. Fast tests do not use Testcontainers or broker connections. Failsafe's integration tests require Docker and do not set `disabledWithoutDocker` or a skip profile.

`docker --context default compose config --quiet` passed. An isolated Compose project (`mqtt-kafka-bridge-foundation-check`) started with `up -d --wait`, and both brokers reported healthy. The packaged application then started with the local profile on host port 18080; `GET /actuator/health` returned `UP`. The application and temporary validation containers were stopped and removed afterward; unrelated containers were preserved.

Production consists solely of bootstrap, validated configuration, disconnected infrastructure and shutdown cleanup. No forwarding, routing, transformations, active listeners, automatic provisioning, watering logic or production Reactor adapters were introduced.

## Generated runtime models follow-up

The user subsequently requested Java message models generated from OpenAPI. Generation is bound to Maven `generate-sources`; generated classes are compiled into the application for runtime use. Application startup does not compile Java or OpenAPI dynamically.

Observed TDD steps:

1. Added mapping/range tests that referenced `com.gladys.bridge.message.SoilMoistureReading`. The test compilation failed because this package/type did not exist.
2. Added the pinned OpenAPI Generator Maven plugin 7.26.0, generating only the three message model classes. Its Boot 4 and Jackson 3 options compile against the existing dependencies. The pinned generator emits JavaBeans, so tests use the generated getters. No custom templates, additional application layers or runtime generation dependencies were added.
3. Added OpenAPI `format: int64` to the ADC count so Java `Long` preserves the already-declared unsigned 32-bit range. The two mapping tests passed, including the exact supplied example and `4_294_967_295` ADC count.
4. Added generated Jakarta validation tests. The supplied example failed validation because JSON Schema's substring pattern `\S` became Java Bean Validation's full-string pattern. Updated the source schema to the equivalent nonblank expression `[\s\S]*\S[\s\S]*`, which works under both validators. No generated source was hand-edited.
5. `./mvnw -B -ntp verify` passed with **76 fast tests and 2 broker integration tests**, zero failures/errors/skips. All four generated-model tests passed: JSON mapping, full ADC range, supplied-example validation and nested validation failures.
6. Inspected the packaged JAR and confirmed all three message model classes and their enum types under `BOOT-INF/classes/com/gladys/bridge/message/`.

OpenAPI remains authoritative. Generated Java lives under ignored `target/generated-sources/openapi/src/main/java/` and is regenerated automatically by Maven. The generator's OpenAPI 3.1 support reports a beta warning; tests cover these concrete schemas, and generated Jakarta annotations do not claim to implement every JSON Schema rule. There is still no production forwarding, routing or payload processing pipeline.

## OpenAPI resources relocation

Moved the OpenAPI document and supplied example from `docs/openapi/` to `src/main/resources/openapi/`. Updated the Maven generator input, existing contract/mapping/broker tests, README and agent guidance. Before moving the files, the updated contract tests failed against the new paths (1 assertion failure and 24 missing-file errors), confirming they required the new location. After relocation, `./mvnw -B -ntp verify` passed with 76 fast tests and 2 broker integration tests, zero failures/errors/skips. JAR inspection confirmed both `openapi/messages.yml` and `openapi/examples/soil-moisture-reading.json` under `BOOT-INF/classes/`.
