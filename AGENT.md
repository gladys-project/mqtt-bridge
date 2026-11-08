# mqtt-kafka-bridge guidance

This service is the MQTT/Kafka transport boundary for the Gladys IoT plant-watering project. Its current scope is bootstrap, validated configuration and broker infrastructure only. Do not implement routing, forwarding, topic-template expansion, device management, notifications, reading analysis, watering decisions or pump logic without a later request.

Use Java 25 without preview features, stable supported Spring Boot, WebFlux/Reactor, Spring Kafka, Eclipse Paho's asynchronous MQTT 3.1.1 client, Maven Wrapper, JUnit Jupiter, Reactor Test and Testcontainers. Keep one Maven module. Do not introduce Spring MVC, Reactor Kafka, Spring Cloud Stream, Lombok, databases or Kubernetes.

Follow TDD: write a meaningful test, run it and verify its expected failure before implementing the smallest passing change. Report actual red/green results. Keep the design simple; avoid speculative interfaces, empty services, adapters and generic frameworks. Use constructor injection and immutable typed configuration properties.

Run `./mvnw test` for Docker-independent fast tests. Run `./mvnw verify` for all checks, including real Kafka and Mosquitto integration tests; Docker must be available and integration tests must fail rather than silently skip when it is absent. Keep test settings isolated from developer profiles. Pin container tags and unmanaged dependencies; let Spring Boot manage compatible libraries and plugins.

Keep the web runtime reactive. Spring Kafka and Paho are asynchronous, not natively reactive, and do not provide end-to-end backpressure. No `.block()`, `.join()`, `Thread.sleep()` or blocking broker calls in production request/event paths. Bounded blocking cleanup is allowed only during application shutdown. Future Reactor adaptation must be lazy and preserve errors. Do not add adapters just to fill the scaffold.

Use `spring.kafka.*` for standard Kafka settings and validated `bridge.*` properties for MQTT and topic declarations. Creating infrastructure must not connect, subscribe, publish, create topics or require reachable brokers. Do not add listeners or startup runners. Anonymous MQTT is allowed for local development/tests; never require credentials for that case. Keep real secrets out of the repository and logs; `.env.example` contains placeholders only. Environment variables must be supplied to the process; Spring Boot does not load `.env` automatically.

Update meaningful tests and documentation with relevant changes. Preserve unrelated work. All connectivity orchestration belongs in tests in this iteration.

Define message structures in `src/main/resources/openapi/messages.yml`, using OpenAPI reusable component schemas. Preserve the supplied soil-moisture MQTT example in `src/main/resources/openapi/examples/soil-moisture-reading.json`. Update contract tests and examples with schema changes. Treat `observedAt` as epoch milliseconds and `rawMoisture` as ADC counts. Generate Java message models from OpenAPI during Maven generate-sources and package them for runtime use, as explicitly requested. Never hand-edit generated sources in target/generated-sources/openapi. Do not add controllers, forwarding, payload processing or validation pipelines merely to represent these contracts. Obtain concrete formats before defining device-event or future command payload contracts.
