# mqtt-kafka-bridge

Configuration-only foundation for the Gladys IoT plant-watering project. Includes a reactive Spring Boot application, Actuator, validated immutable properties, Boot-managed Kafka producer/consumer factories and `KafkaTemplate`, and a disconnected Paho asynchronous MQTT client with MQTT 3.1.1 options and bounded shutdown.

There is no forwarding, subscription, publishing, active Kafka listener, topic provisioning or business processing in production code. Startup requires no reachable brokers. Device management, errors and notifications belong to the device service. Reading analysis, calibration, watering decisions and pump requests belong to the telemetry service.

## Setup and local startup

Install JDK 25 and Docker Engine with Compose v2. Maven is downloaded by the checked-in wrapper; no global Maven installation is needed. From the repository root:

```bash
./mvnw test
# Start the two local brokers and wait for readiness:
docker compose up -d --wait
# Run the application on the host, using local broker addresses:
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

In another terminal:

```bash
curl --fail http://localhost:8080/actuator/health
curl --fail http://localhost:8080/actuator/info
# When finished:
docker compose down
```

The application can also start with brokers stopped. Kafka and MQTT clients stay idle until explicitly used. Health reports application health, not broker connectivity. Only health and info are exposed by default; there are no custom controllers or health indicators.

Compose runs a single Kafka KRaft node and Mosquitto. Kafka advertises `localhost:9092` for host clients and `kafka:29092` internally. Both host ports bind to loopback. Mosquitto's anonymous access and Kafka's plaintext listeners are for this local setup only. Topics are not automatically created. No persistent volumes are configured; this is disposable development infrastructure.

If your Docker CLI selects an unavailable Docker Desktop daemon while the system daemon is running, use `docker --context default compose ...` and run integration tests with `DOCKER_HOST=unix:///var/run/docker.sock ./mvnw verify`. Otherwise use your normal Docker endpoint.

## Configuration

`src/main/resources/application.yml` contains shared defaults; `application-local.yml` declares host-to-Compose addresses. Tests explicitly load `application-test.yml`, bypassing developer configuration and credential placeholders. Standard Spring Boot external configuration precedence applies; environment variables override YAML.

| Environment variable | Default / purpose |
| --- | --- |
| `SPRING_KAFKA_BOOTSTRAPSERVERS` | `localhost:9092` |
| `SPRING_KAFKA_CONSUMER_GROUPID` | `mqtt-kafka-bridge-commands` |
| `SPRING_KAFKA_SECURITY_PROTOCOL` | Kafka client default `PLAINTEXT` |
| `SPRING_KAFKA_PROPERTIES_SASL_MECHANISM` | Optional broker SASL mechanism |
| `SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG` | Optional JAAS credentials/configuration |
| `BRIDGE_MQTT_BROKERURI` | `tcp://localhost:1883`; `tcp` or TLS `ssl`, host and optional valid port, no URI credentials/path/query/fragment |
| `BRIDGE_MQTT_CLIENTID` | `mqtt-kafka-bridge`; use a unique nonblank ID per instance |
| `MQTT_USERNAME`, `MQTT_PASSWORD` | Empty, for anonymous local access |
| `BRIDGE_MQTT_USERNAME`, `BRIDGE_MQTT_PASSWORD` | Direct property overrides; take precedence over the credential aliases above |
| `BRIDGE_MQTT_QOS` | `1`; accepted values `0`, `1`, `2` |
| `BRIDGE_MQTT_CONNECTIONTIMEOUT` | `5s`; 1 second to 2,147,483,647 seconds; Paho uses whole seconds |
| `BRIDGE_MQTT_SHUTDOWNTIMEOUT` | `5s`; 1 millisecond to 60 seconds |
| `BRIDGE_MQTT_KEEPALIVE` | `30s`; 1 to 65,535 seconds; Paho uses whole seconds |
| `SERVER_PORT` | `8080` |

Use standard `spring.kafka.*` properties for Kafka security, TLS and client tuning. Producer keys are strings, values are bytes and acknowledgements are `all`. The future consumer factory uses string keys and byte values, disables auto commit and automatic topic creation, and starts no listener. Kafka admin auto creation is disabled.

`.env.example` contains placeholders only. Spring Boot does **not** automatically load `.env`; Docker Compose's `.env` substitution also does not export values to a host JVM. For anonymous local use, no env file is needed. For custom settings, copy the example to ignored `.env`, replace the placeholders, quote values containing spaces or shell metacharacters, remove unset optional variables, then explicitly export it in your shell:

```bash
set -a
. ./.env
set +a
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

Only source an env file you control. Keep actual credentials out of Git and logs. For remote MQTT TLS use `ssl://` and a JVM trust store suitable for your broker certificate.

## Topic declarations

| Purpose | Default | Environment override |
| --- | --- | --- |
| MQTT telemetry ingress | `gladys/devices/+/telemetry` | `BRIDGE_MQTT_TELEMETRYTOPIC` |
| MQTT device-event ingress | `gladys/devices/+/events` | `BRIDGE_MQTT_DEVICEEVENTTOPIC` |
| MQTT command template | `gladys/devices/{deviceId}/commands` | `BRIDGE_MQTT_COMMANDTOPICTEMPLATE` |
| Kafka telemetry destination | `gladys.telemetry.raw.v1` | `BRIDGE_TOPICS_TELEMETRY` |
| Kafka device-event destination | `gladys.device.events.v1` | `BRIDGE_TOPICS_DEVICEEVENTS` |
| Kafka future command source | `gladys.device.commands.v1` | `BRIDGE_TOPICS_COMMANDS` |

These are configuration declarations only. MQTT filters validate wildcard placement; command templates require exactly one `{deviceId}` topic level and no wildcards. Kafka topic names use valid characters, at most 249 characters, and exclude `.` and `..`.

Telemetry and device events have **separate** intended destinations. An incoming message is not assumed to be duplicated to both topics. The intended Kafka key is device ID for per-device ordering within a partition. Template expansion and routing are future work.

## Message contracts

[OpenAPI 3.1.2 schemas](src/main/resources/openapi/messages.yml) define the supplied `SOIL_MOISTURE_READING` MQTT structure, its payload and nested device errors. The [exact supplied JSON example](src/main/resources/openapi/examples/soil-moisture-reading.json) is checked by fast tests and transported unchanged in the broker tests.

The v1 schema requires all fields in that example, UUID event/device/boot IDs, nonnegative sequence/uptime/epoch-millisecond observation time, battery percentage 0–100, and raw ADC counts with resolution 1–32 bits. It accepts an empty errors array. Objects reject undeclared fields. The initial severity vocabulary is `INFO`, `WARNING`, `ERROR`, `CRITICAL`; clock-source and quality identifiers remain open uppercase vocabularies. These are initial contract constraints, not runtime bridge behavior. Relationships between ADC counts, resolution and calibration belong to the telemetry service.

An embedded `LOW_BATTERY` diagnostic does not change this telemetry message into a separate device-event message. Device-event and future pump-command payload contracts await concrete formats; their topics are already declared. OpenAPI is used as a reusable schema document with empty HTTP paths. The OpenAPI document and example are packaged as classpath resources under `openapi/`. Maven generates `SoilMoistureReading`, `SoilMoisturePayload` and `DeviceError` in package `com.gladys.bridge.message` from this OpenAPI file during `generate-sources`, before compilation. Their compiled classes are included in the application JAR and available at runtime. Generated code uses Jakarta Validation and Jackson annotations compatible with Boot 4/Jackson 3. UUID fields map to `UUID`, epoch milliseconds to `Long`, and raw ADC counts to `Long` to preserve the declared unsigned 32-bit range. The application’s configured Jackson mapper round-trips the supplied example without changing its JSON structure.

```bash
./mvnw generate-sources
# Generated Java sources:
# target/generated-sources/openapi/src/main/java/com/gladys/bridge/message/
```

`test`, `verify`, `package` and `spring-boot:run` run the generation phase automatically. Keep OpenAPI as the source of truth and edit the schema rather than generated files. Generated sources stay under ignored `target/`; generation clears its own output directory to remove stale model files. This is build-time generation for runtime use. The running JVM does not compile schemas or Java source at startup. The generator emits models only; there are no generated HTTP clients/controllers or runtime payload processing pipelines. Full contract validation remains in tests; generated Jakarta constraints are not a replacement for every JSON Schema rule. The pinned generator reports its OpenAPI 3.1 support as beta; the concrete generated mappings and constraints used here are covered by tests.

## Tests and reactive boundaries

```bash
./mvnw test      # Surefire: *Test, no Docker or running brokers
./mvnw verify    # Also Failsafe: *IT; Docker is REQUIRED
```

Fast tests cover property binding and environment-style overrides, invalid configuration, infrastructure/options, shutdown success/failure, broker-independent startup, HTTP health/exposure, OpenAPI contracts, and generated-model mapping/validation with the application’s Jackson mapper. Broker tests start pinned real containers with explicit readiness checks, inject mapped addresses, explicitly provision a test-only Kafka topic, verify a record's key and payload, await MQTT SUBACK before publishing and verify delivery. Context closure exercises the connected MQTT client's configured cleanup. Missing Docker fails verification; tests are not silently skipped.

All connectivity publishers, consumers and callbacks are test code. Waits are bounded. `StepVerifier` checks the actual WebFlux HTTP response publisher. Spring Kafka and Paho are asynchronous integrations, not natively reactive; there is no claim of end-to-end backpressure. No blocking calls exist in production request/event paths. MQTT shutdown alone uses a bounded token wait, then forces a local disconnect without sending another network packet and closes the client. Clean sessions, in-memory MQTT persistence and disabled automatic reconnect keep this iteration minimal. Future Reactor adaptation must be lazy and preserve errors.

## Pinned versions and compatibility

| Component | Version / policy |
| --- | --- |
| Java | 25, `--release 25`, no preview |
| Spring Boot / Boot Maven plugin | 4.1.2 |
| Spring Kafka / Kafka clients | 4.1.2 / 4.2.2, Boot managed |
| Reactor Core / Reactor Test | 3.8.8, Boot managed |
| JUnit Jupiter / Testcontainers | 6.0.3 / 2.0.5, Boot managed |
| Paho Java MQTT v3 client | 1.2.5 |
| Maven / Wrapper | 3.9.12 / 3.3.4, Maven distribution SHA-256 recorded |
| Compiler / Surefire / Failsafe plugins | 3.15.0 / 3.5.6 / 3.5.6, Boot managed |
| Kafka / Mosquitto images | `apache/kafka:4.1.0` / `eclipse-mosquitto:2.0.22` |
| OpenAPI Generator Maven plugin | 7.26.0, pinned; models only, Spring Boot 4 / Jackson 3 |
| OpenAPI / Swagger Parser / JSON Schema validator | 3.1.2 / 2.1.48 / NetworkNT 3.0.9; validators are test-only |

Official references checked for this foundation:

- [Spring Boot stable release and support](https://spring.io/projects/spring-boot/) and [system requirements](https://docs.spring.io/spring-boot/system-requirements.html): Java 25 is supported; Maven 3.6.3+ is required.
- [Boot-managed dependencies](https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html), [Kafka auto-configuration](https://docs.spring.io/spring-boot/reference/messaging/kafka.html) and [Boot build-plugin management](https://docs.spring.io/spring-boot/appendix/dependency-versions/properties.html).
- [Paho 1.2.5 release](https://github.com/eclipse-paho/paho.mqtt.java/releases/tag/v1.2.5), [asynchronous client API](https://eclipse.dev/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttAsyncClient.html) and [bounded token completion](https://eclipse.dev/paho/files/javadoc/org/eclipse/paho/client/mqttv3/IMqttToken.html).
- [Maven Wrapper](https://maven.apache.org/tools/wrapper/index.html), [wrapper plugin](https://maven.apache.org/tools/wrapper/maven-wrapper-plugin/wrapper-mojo.html), [compiler release targeting](https://maven.apache.org/guides/mini/guide-java-target.html), [Surefire](https://maven.apache.org/surefire/maven-surefire-plugin/) and [Failsafe lifecycle](https://maven.apache.org/surefire/maven-failsafe-plugin/).
- [Testcontainers Kafka module](https://java.testcontainers.org/modules/kafka/), [Apache Kafka Docker images](https://kafka.apache.org/41/getting-started/docker/), [Mosquitto official image](https://hub.docker.com/_/eclipse-mosquitto) and [Mosquitto configuration](https://mosquitto.org/man/mosquitto-conf-5.html).
- [OpenAPI Generator Maven lifecycle](https://openapi-generator.tech/docs/plugins/) and [Spring generator Boot 4 / Jackson 3 options](https://openapi-generator.tech/docs/generators/spring/).
- [OpenAPI 3.1.2 schema objects](https://spec.openapis.org/oas/v3.1.2.html), [Swagger Parser 3.1 support](https://github.com/swagger-api/swagger-parser) and [NetworkNT validator Java/Jackson compatibility](https://github.com/networknt/json-schema-validator): the 3.x validator supports Java 17+ and Jackson 3 used by Boot 4.

The actual red/green steps and final check results are recorded in [docs/verification.md](docs/verification.md).

See [AGENT.md](AGENT.md) for authoritative project guidance; [AGENTS.md](AGENTS.md) points to it for automatic agent discovery.
