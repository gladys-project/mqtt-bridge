package com.gladys.bridge.config.properties;

import java.nio.charset.StandardCharsets;

import org.eclipse.paho.client.mqttv3.MqttTopic;

final class MqttTopicValidation {
    private MqttTopicValidation() {
    }

    static boolean isUtf8String(String value) {
        return value != null && !value.isEmpty() && value.indexOf('\0') < 0
                && StandardCharsets.UTF_8.newEncoder().canEncode(value)
                && value.getBytes(StandardCharsets.UTF_8).length <= 65535;
    }

    static boolean isTopic(String value, boolean filter) {
        if (!isUtf8String(value) || value.isBlank()) {
            return false;
        }
        try {
            MqttTopic.validate(value, filter);
            return true;
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }
}
