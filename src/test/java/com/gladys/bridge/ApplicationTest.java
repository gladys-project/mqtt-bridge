package com.gladys.bridge;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.config.location=classpath:/application-test.yml")
class ApplicationTest {
    private final WebTestClient web;

    @Autowired
    ApplicationTest(@LocalServerPort int port) {
        web = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void startsWithoutBrokersAndServesHealth() {
        web.get().uri("/actuator/health").exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("UP");
    }

    @Test
    void healthResponseBodyIsARealReactivePublisher() {
        var response = web.get().uri("/actuator/health").exchange()
                .expectStatus().isOk().returnResult(Map.class);
        StepVerifier.create(response.getResponseBody())
                .assertNext(body -> assertThat(body.get("status")).isEqualTo("UP"))
                .expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void exposesOnlyHealthAndInfo() {
        web.get().uri("/actuator/info").exchange().expectStatus().isOk();
        web.get().uri("/actuator/env").exchange().expectStatus().isNotFound();
        web.get().uri("/actuator/beans").exchange().expectStatus().isNotFound();
    }
}
