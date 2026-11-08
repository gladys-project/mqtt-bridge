package com.gladys.bridge.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("bridge")
@Validated
public record BridgeProperties(@NotNull @Valid Topics topics) {
    public record Topics(
            @NotNull @Pattern(regexp = "(?!\\.{1,2}$)[a-zA-Z0-9._-]{1,249}") String telemetry,
            @NotNull @Pattern(regexp = "(?!\\.{1,2}$)[a-zA-Z0-9._-]{1,249}") String deviceEvents,
            @NotNull @Pattern(regexp = "(?!\\.{1,2}$)[a-zA-Z0-9._-]{1,249}") String commands) {
    }
}
