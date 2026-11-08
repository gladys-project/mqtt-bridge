package com.gladys.bridge.config.properties;

import java.net.URI;
import java.time.Duration;
import java.util.Set;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("bridge.mqtt")
@Validated
public record MqttProperties(
        @NotBlank String brokerUri,
        @NotBlank String clientId,
        String username,
        String password,
        @Min(0) @Max(2) int qos,
        @NotNull @DurationMin(seconds = 1) @DurationMax(seconds = Integer.MAX_VALUE) Duration connectionTimeout,
        @NotNull @DurationMin(millis = 1) @DurationMax(seconds = 60) Duration shutdownTimeout,
        @NotNull @DurationMin(seconds = 1) @DurationMax(seconds = 65535) Duration keepAlive,
        @NotBlank String telemetryTopic,
        @NotBlank String deviceEventTopic,
        @NotBlank String commandTopicTemplate) {

    @AssertTrue(message = "broker URI must be tcp:// or ssl:// with a host, valid optional port and no credentials, path, query or fragment")
    public boolean isBrokerUriValid() {
        try {
            var uri = URI.create(brokerUri);
            return Set.of("tcp", "ssl").contains(uri.getScheme())
                    && uri.getHost() != null && uri.getUserInfo() == null
                    && (uri.getPort() == -1 || uri.getPort() > 0 && uri.getPort() <= 65535)
                    && (uri.getRawPath() == null || uri.getRawPath().isEmpty())
                    && uri.getRawQuery() == null && uri.getRawFragment() == null;
        } catch (IllegalArgumentException | NullPointerException ignored) {
            return false;
        }
    }

    @AssertTrue(message = "client ID must be a nonempty MQTT UTF-8 string of at most 65535 bytes")
    public boolean isClientIdValid() {
        return MqttTopicValidation.isUtf8String(clientId);
    }

    @AssertTrue(message = "telemetry topic must be a valid MQTT subscription filter")
    public boolean isTelemetryTopicValid() {
        return MqttTopicValidation.isTopic(telemetryTopic, true);
    }

    @AssertTrue(message = "device-event topic must be a valid MQTT subscription filter")
    public boolean isDeviceEventTopicValid() {
        return MqttTopicValidation.isTopic(deviceEventTopic, true);
    }

    @AssertTrue(message = "command topic template must contain exactly one {deviceId} level and no wildcards or other placeholders")
    public boolean isCommandTopicTemplateValid() {
        if (!MqttTopicValidation.isTopic(commandTopicTemplate, false)) {
            return false;
        }
        int placeholders = 0;
        for (String level : commandTopicTemplate.split("/", -1)) {
            if (level.equals("{deviceId}")) {
                placeholders++;
            } else if (level.contains("{") || level.contains("}")) {
                return false;
            }
        }
        return placeholders == 1;
    }

    @Override
    public String toString() {
        return "MqttProperties[clientId=" + clientId + ", credentials=<redacted>]";
    }
}
