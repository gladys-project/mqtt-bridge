# mqtt-kafka-bridge

Gladys MQTT/Kafka transport foundation with generated message contracts and mapper, a configured HiveMQ MQTT 5 client, and an explicit telemetry processor. `MqttClientConfig` owns identifier/host/port and the validated subscription topic/QoS. The processor receives its client, mapper and Kafka destination through constructor injection. It converts each MQTT observation into one complete JSON Kafka record keyed by deviceId. Application startup stays disconnected; connection and subscription are explicit operations.

The intended architecture is Arduino → MQTT → bridge → **one Kafka raw telemetry topic**. Separate downstream Device Processor and Telemetry Processor applications consume that topic with different consumer groups and produce their own outputs. They own device handling, notifications, calibration and watering logic. Soil is resolved downstream from `sensorId`; a device can report multiple sensors in different soils. No `soilId` is required.

## Message definitions

Two OpenAPI 3.1.2 files under `src/main/resources/openapi/` define the bridge boundaries:

| Boundary | Definition | Generated DTO |
| --- | --- | --- |
| MQTT inbound | [mqtt-inbound.yml](src/main/resources/openapi/mqtt-inbound.yml) | `MqttTelemetryMessage` |
| Kafka raw outbound | [kafka-raw-telemetry.yml](src/main/resources/openapi/kafka-raw-telemetry.yml) | `KafkaRawTelemetry` |

Both contain the complete observation: envelope, battery, errors and sensor payload. The [original MQTT example](src/main/resources/openapi/examples/soil-moisture-reading.json) and [Kafka example](src/main/resources/openapi/examples/kafka-raw-telemetry.json) have identical content. There are no separate device/telemetry projections in this bridge. The intended Kafka key is `deviceId` for per-device partition ordering. Event ID, boot ID and sequence support downstream correlation/deduplication.

Schemas declare version 1, UUID event/device/boot IDs, nonnegative sequence/uptime/epoch-millisecond timestamp, uppercase event type and clock source, mock flag, battery percentage 0–100, diagnostics and payload. `payload.sensorId` is required. Plant/calibration IDs are optional metadata; measurement fields remain extensible for different sensors. Raw moisture stays an ADC count, not a percentage. Downstream services interpret measurements and resolve sensor-to-soil associations.

Maven generates message DTOs during `generate-sources`, then compiles/packages them for runtime use. MapStruct generates the Spring message mapper during compilation. The JVM does not generate code at startup. Edit the OpenAPI sources, never generated files under ignored `target/`. No controller, transport or message processor code is generated. Contract tests check JSON Schema rules; generated Jakarta Validation covers a subset. The pinned generator warns that OpenAPI 3.1 support is beta; concrete examples and generated DTO behavior are tested.

## MQTT-to-Kafka mapper

Constructor-inject `com.gladys.bridge.mapping.MqttToKafkaMapper` and call:

```java
KafkaRawTelemetry outbound = mqttToKafkaMapper.toKafka(inbound);
```

`inbound` is the generated `MqttTelemetryMessage`. The mapper preserves the complete envelope, device ID, diagnostics, sensor ID and extensible measurement fields in one output. Soil resolution remains downstream from sensorId. It copies diagnostic objects, the payload object and its measurement map; it performs no interpretation, broker I/O or JSON decoding. Null input maps to null. Runtime validation remains a separate concern.

MapStruct 1.6.3 generates the Spring implementation under `target/generated-sources/annotations/`. Unmapped source/target properties fail compilation; extensible measurement fields are copied through the generated DTO's any-setter. Lombok/MapStruct binding 0.2.0 coordinates the annotation processors. Do not edit generated code.

## Setup and independent Compose stacks

Install Java 25 and Docker with Compose v2. Maven Wrapper supplies Maven. The stacks have separate lifecycles:

| File | Services | Purpose |
| --- | --- | --- |
| [compose.yml](compose.yml) | mqtt-kafka-bridge only | Build/run this application |
| [infra/compose.yml](infra/compose.yml) | Kafka and Mosquitto only | Independent local broker infrastructure |

Both use the external Docker network `gladys` (override with `GLADYS_NETWORK`). Create it once; neither Compose project owns or deletes it:

```bash
docker network inspect gladys >/dev/null 2>&1 || docker network create gladys
docker compose -f infra/compose.yml up -d --wait
```

For a **host-run application**, the local profile uses `localhost:9092` and `localhost:1883`:

```bash
./mvnw test
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

For the **application container**, create an ignored `.env` beside the root `compose.yml` from `.env.example` if it does not exist, replace the placeholders, and use these values for the supplied broker stack:

```dotenv
SPRING_KAFKA_BOOTSTRAPSERVERS=kafka:29092
BRIDGE_MQTT_IDENTIFIER=mqtt-kafka-bridge
BRIDGE_MQTT_HOST=mosquitto
BRIDGE_MQTT_PORT=1883
```

Then build the verified executable JAR and the service image:

```bash
./mvnw clean verify
docker compose up -d --build
curl --fail --retry 20 --retry-connrefused --retry-delay 1 http://localhost:8080/actuator/health
```

The [Dockerfile](Dockerfile) uses a digest-pinned Temurin Java 25 JRE and runs as UID/GID 10001. It copies an already-built JAR; Maven is not installed in the runtime image. The [.dockerignore](.dockerignore) allows only the Dockerfile and executable JAR into the build context, excluding environment files and Git data. A release JAR can instead be placed at `target/mqtt-kafka-bridge-<version>.jar` before building; keep only one matching JAR there.

The root stack has no broker services or `depends_on` entries. `docker compose down` stops only this application. Stop the independent development brokers with `docker compose -f infra/compose.yml down`. The shared network remains in place. Application startup stays disconnected; connection/subscription behavior remains explicit.

Kafka advertises `localhost:9092` for host clients and `kafka:29092` for containers on the shared network. Mosquitto uses `localhost:1883` from the host and `mosquitto:1883` from containers. `localhost` inside the bridge container refers to that container, so use broker service names there. Kafka's internal advertised address must be reachable from the bridge. Other independently managed brokers can be selected through the environment values.

The supplied infrastructure Compose is disposable local development infrastructure: Kafka KRaft/plaintext and anonymous Mosquitto, loopback-only host ports, no persistent volumes or automatic topic creation. Manage secured/persistent server brokers independently. For local message publishing, provision the raw topic in the infrastructure stack first:

```bash
docker compose -f infra/compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic gladys.telemetry.raw.v1 --partitions 1 --replication-factor 1
```

Host ports can be changed with `KAFKA_HOST_PORT`, `MQTT_HOST_PORT` and `BRIDGE_HTTP_PORT`. The application container always listens on 8080. `BRIDGE_IMAGE` optionally selects its image name. If Docker Desktop is unavailable but the system daemon works, use `docker --context default compose ...` and `DOCKER_HOST=unix:///var/run/docker.sock ./mvnw verify`.

## Server environment file

Keep the real server environment outside the checkout at **`/etc/gladys/mqtt-kafka-bridge.env`**, readable only by its deployment operator. For a root-managed deployment:

```bash
sudo install -d -m 750 /etc/gladys
# Create this file once from the placeholder example, then replace every placeholder:
sudo install -m 600 .env.example /etc/gladys/mqtt-kafka-bridge.env
sudoedit /etc/gladys/mqtt-kafka-bridge.env
```

Do not overwrite an existing server env file during subsequent deployments. For brokers on the same shared Docker network, use `kafka:29092` and `mosquitto:1883` as above; for separately hosted brokers, use their reachable hostnames and advertised Kafka ports. Keep a unique MQTT identifier per instance.

Assuming the project and verified JAR are deployed under `/opt/gladys/mqtt-kafka-bridge`, start only this service:

```bash
sudo env BRIDGE_ENV_FILE=/etc/gladys/mqtt-kafka-bridge.env \
  docker compose --env-file /etc/gladys/mqtt-kafka-bridge.env \
  -f /opt/gladys/mqtt-kafka-bridge/compose.yml up -d --build
```

`BRIDGE_ENV_FILE` selects the file used by the service's `env_file`, which passes its values to the JVM. The CLI `--env-file` also supplies Compose interpolation values such as `GLADYS_NETWORK` or `BRIDGE_HTTP_PORT`. For local development the default service env file is `.env` beside `compose.yml`. Spring Boot does not itself load either file. See Docker's [container environment configuration](https://docs.docker.com/compose/how-tos/environment-variables/set-environment-variables/) and [external network guidance](https://docs.docker.com/compose/how-tos/networking/).

Create/join the selected external network before startup; brokers do not have to be in the service's Compose project. This command builds from the deployed verified JAR; it does not download a release or start broker infrastructure. Only health/info are exposed, with HTTP bound to server loopback by default.

## Configuration

Shared defaults are in `application.yml`, local addresses in `application-local.yml`, and fast tests load isolated `application-test.yml`. The identifier, host and port configure the disconnected client bean; QoS and telemetry topic configure the subscription request. The shared YAML contains only settings used by the current implementation.

| Environment variable | Default / meaning |
| --- | --- |
| `SPRING_KAFKA_BOOTSTRAPSERVERS` | `localhost:9092` |
| `BRIDGE_MQTT_IDENTIFIER` | `mqtt-kafka-bridge`; unique client identifier |
| `BRIDGE_MQTT_HOST` | `localhost`; broker hostname or IP, without scheme or port |
| `BRIDGE_MQTT_PORT` | `1883`; broker port |
| `BRIDGE_MQTT_TELEMETRYTOPIC` | `gladys/devices/+/telemetry`, intended MQTT subscription filter |
| `BRIDGE_TOPICS_TELEMETRY` | `gladys.telemetry.raw.v1`, intended Kafka output |
| `BRIDGE_MQTT_QOS` | `1`; accepts 0, 1, 2 |
| `SERVER_PORT` | `8080` |

Standard Kafka broker/security/client settings stay under `spring.kafka.*`; examples include `SPRING_KAFKA_SECURITY_PROTOCOL`, `SPRING_KAFKA_PROPERTIES_SASL_MECHANISM` and `SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG`. Boot supplies producer/consumer factories and `KafkaTemplate`, with string keys and byte values. There is no active Kafka consumer/listener. Kafka provisioning stays disabled.

Obsolete Cloud Stream function definitions/bindings and unused MQTT credentials/timeouts were removed. The existing Stream dependency remains installed, with function autodetection disabled; the processor uses KafkaTemplate directly. Production YAML has no Kafka consumer/listener settings because this bridge publishes to Kafka. Test-only consumer settings stay in `application-test.yml`. Current MQTT connection settings support anonymous local access; authentication/TLS lifecycle configuration requires a later implementation.

`.env.example` contains placeholders only. Spring Boot does not automatically load `.env`, and Compose does not export it to a host JVM. Copy to ignored `.env`, replace placeholders, quote shell-sensitive values, remove unused optional variables, then explicitly export a file you control:

```bash
set -a
. ./.env
set +a
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

Keep secrets out of Git/logs. Lombok remains compile-time only; SLF4J uses Boot's default Logback backend.

## Subscription and checks

The subscription request is a `Mqtt5Subscribe` bean built from `bridge.mqtt.telemetry-topic` and `bridge.mqtt.qos`. QoS defaults to `1` (`AT_LEAST_ONCE`); only `0`, `1`, `2` are accepted. Invalid MQTT filters fail configuration. Kafka output uses `bridge.topics.telemetry`. Override them through `BRIDGE_MQTT_QOS`, `BRIDGE_MQTT_TELEMETRYTOPIC` and `BRIDGE_TOPICS_TELEMETRY`.

Connect first, then explicitly subscribe; the returned future completes with SUBACK:

```java
mqttClientConfig.connectSubscriber(mqttClient)
        .thenCompose(connAck -> telemetryReadingProcessor.subscribe());
```

The processor uses KafkaTemplate with string device ID keys and JSON byte values, matching the configured serializers. It decodes UTF-8 JSON directly from MQTT bytes, maps the entire observation, and observes asynchronous send failures. Invalid JSON is logged and not sent. The caller owns connection/disconnection; no startup hook or Cloud function automatically starts the subscription. The Cloud function declarations remain inactive and are separate from this explicit KafkaTemplate path.

```bash
./mvnw test          # Fast tests, Docker independent
./mvnw verify        # Kafka and Mosquitto integration tests; Docker REQUIRED
./mvnw clean verify  # Clears obsolete generated models/classes/resources
```

Fast tests cover client/environment settings, valid/invalid QoS and topic filters, broker-independent startup/health, contracts, mapper behavior, subscription completion/error propagation, complete JSON forwarding to the configured Kafka destination, and asynchronous send errors. Integration tests use pinned real containers: a Kafka key/payload check and a HiveMQ MQTT-to-Kafka flow with custom topics and QoS 2. The latter waits for SUBACK, asserts broker-granted QoS, checks the complete record and device key, then explicitly disconnects. Failsafe never silently skips when Docker is absent.

MQTT QoS applies to the MQTT leg. Kafka forwarding is asynchronous and does not form an atomic transaction with MQTT acknowledgement; failures are logged, with no durable retry/replay or exactly-once guarantee across brokers. WebFlux remains reactive; these integrations do not provide end-to-end backpressure. No `.block()`, `.join()`, sleeps or explicit blocking waits appear in production event code. Device management, sensor/soil resolution, watering and pump logic remain outside this bridge.

## CI/CD

GitHub Actions workflows live under [.github/workflows](.github/workflows/):

- **CI** runs on branch pushes, pull requests and manual dispatch. It sets up Temurin Java 25, caches Maven dependencies, requires Docker, runs `./mvnw --batch-mode --no-transfer-progress clean verify`, validates both Compose files and builds the service image. Test reports are retained for 7 days and the verified executable JAR for 14 days. It is also callable by the release workflow.
- **Release** runs for pushed `v*` tags, calls the same verification workflow, downloads its exact JAR artifact and creates a GitHub Release with that artifact and generated notes. Publication gets `contents: write` only in the publish job; test/build jobs have read-only repository access. No additional secret is required beyond the workflow's GitHub token.

CI uses the selected **self-hosted** runner. Provide a Linux runner with a current GitHub Actions runner service (Node 24 action support), Docker daemon access, Git, a shell and outbound access for Java/Maven/image downloads. Java is provisioned by setup-java; Maven comes from the wrapper. The release publication job also uses `self-hosted` and requires GitHub CLI (`gh`) on its PATH. Configure runner access and workflow permissions in the repository settings as appropriate for that runner.

Actions are pinned to immutable commits: checkout 6.0.3, setup-java 6.2.0, upload-artifact 7.0.2 and download-artifact 8.0.2. Workflows were checked locally with actionlint 1.7.12 and an artifact name/path check against the Maven build; remote Actions runs begin after these files are pushed. No workflow was triggered or release published during local verification.

Publish a release when the tagged commit is ready (update the POM version first if needed):

```bash
git tag v0.0.1
git push origin v0.0.1
```

The JAR retains the version declared in `pom.xml`. Existing releases/assets are not overwritten automatically. CD delivers a verified JAR to GitHub Releases. Container deployment uses the root Compose file and server env file described above; no automatic server deployment or container registry publication is configured.

## Pinned versions and compatibility

| Component | Version / policy |
| --- | --- |
| Java | 25, `--release 25`, no preview |
| Spring Boot / Boot Maven plugin | 4.1.2 |
| Spring Kafka / Kafka clients | 4.1.2 / 4.2.2, Boot managed |
| Reactor Core / Reactor Test | 3.8.8, Boot managed |
| JUnit Jupiter / Testcontainers | 6.0.3 / 2.0.5, Boot managed |
| Maven / Wrapper | 3.9.12 / 3.3.4, Maven distribution SHA-256 recorded |
| Compiler / Surefire / Failsafe plugins | 3.15.0 / 3.5.6 / 3.5.6, Boot managed |
| Service runtime image | `eclipse-temurin:25-jre`, digest pinned in Dockerfile |
| Kafka / Mosquitto images | `apache/kafka:4.1.0` / `eclipse-mosquitto:2.0.22` |
| HiveMQ MQTT Client | 1.4.1, pinned; MQTT 5 selected by MqttClientConfig |
| Lombok | 1.18.48, Boot managed, compile-time only |
| MapStruct API / processor / Lombok binding | 1.6.3 / 1.6.3 / 0.2.0, pinned |
| SLF4J API | 2.0.20, Boot managed, with Boot’s default Logback backend |
| Spring Cloud BOM / Stream Kafka binder / Function | 2025.1.4 / 5.0.4 / 5.0.5 |
| OpenAPI Generator Maven plugin | 7.26.0, pinned; models only, Spring Boot 4 / Jackson 3 |
| OpenAPI / Swagger Parser / JSON Schema validator | 3.1.2 / 2.1.48 / NetworkNT 3.0.9; validators are test-only |

Official compatibility references:

- [MapStruct 1.6.3 Maven setup](https://mapstruct.org/documentation/installation/) and [mapping/reference guide](https://mapstruct.org/documentation/stable/reference/html/).

- [HiveMQ client 1.4.1 release](https://github.com/hivemq/hivemq-mqtt-client/releases/tag/v1.4.1), [supported protocols and Java requirements](https://github.com/hivemq/hivemq-mqtt-client/tree/v1.4.1). Boot manages shared Netty, Reactor and SLF4J versions. The integration test exercises the selected client with Boot-managed Netty and real Mosquitto.
- [Spring Cloud / Boot compatibility](https://spring.io/projects/spring-cloud/), [functional bindings](https://docs.spring.io/spring-cloud-stream/reference/spring-cloud-stream/producing-and-consuming-messages.html) and [Kafka binder options](https://docs.spring.io/spring-cloud-stream/reference/kafka/kafka-binder/config-options.html).
- [Spring Boot stable release and support](https://spring.io/projects/spring-boot/) and [system requirements](https://docs.spring.io/spring-boot/system-requirements.html): Java 25 is supported; Maven 3.6.3+ is required.
- [Boot-managed dependencies](https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html), [Kafka auto-configuration](https://docs.spring.io/spring-boot/reference/messaging/kafka.html) and [Boot build-plugin management](https://docs.spring.io/spring-boot/appendix/dependency-versions/properties.html).
- [Maven Wrapper](https://maven.apache.org/tools/wrapper/index.html), [wrapper plugin](https://maven.apache.org/tools/wrapper/maven-wrapper-plugin/wrapper-mojo.html), [compiler release targeting](https://maven.apache.org/guides/mini/guide-java-target.html), [Surefire](https://maven.apache.org/surefire/maven-surefire-plugin/) and [Failsafe lifecycle](https://maven.apache.org/surefire/maven-failsafe-plugin/).
- [Testcontainers Kafka module](https://java.testcontainers.org/modules/kafka/), [Apache Kafka Docker images](https://kafka.apache.org/41/getting-started/docker/), [Mosquitto official image](https://hub.docker.com/_/eclipse-mosquitto) and [Mosquitto configuration](https://mosquitto.org/man/mosquitto-conf-5.html).
- [Lombok Maven setup](https://projectlombok.org/setup/maven), [`@Slf4j`](https://projectlombok.org/features/log).
- [OpenAPI Generator Maven lifecycle](https://openapi-generator.tech/docs/plugins/) and [Spring generator Boot 4 / Jackson 3 options](https://openapi-generator.tech/docs/generators/spring/).
- [OpenAPI 3.1.2 schema objects](https://spec.openapis.org/oas/v3.1.2.html), [Swagger Parser 3.1 support](https://github.com/swagger-api/swagger-parser) and [NetworkNT validator Java/Jackson compatibility](https://github.com/networknt/json-schema-validator): the 3.x validator supports Java 17+ and Jackson 3 used by Boot 4.

See [AGENT.md](AGENT.md) for authoritative project guidance; [AGENTS.md](AGENTS.md) points to it for automatic agent discovery.
