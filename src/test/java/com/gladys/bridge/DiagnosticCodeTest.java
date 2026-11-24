package com.gladys.bridge;

import com.gladys.bridge.message.DeviceError;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class DiagnosticCodeTest {
    private static ValidatorFactory factory;
    private static Validator validator;

    @BeforeAll
    static void validation() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @AfterAll
    static void closeValidation() {
        factory.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"BAT_01", "SEN_01", "PMP_01", "A1B_2C"})
    void acceptsStableConditionCodesWithCriticalSeverity(String code) {
        assertThat(validator.validate(diagnostic(code))).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"WARNING", "CRITICAL", "LOW_BATTERY", "", "BAT01", "BAT__01", "BAT_001", "bat_01", "BAT-01"})
    void rejectsSeverityNamesAndMalformedConditionCodes(String code) {
        assertThat(validator.validate(diagnostic(code)))
                .extracting(violation -> violation.getPropertyPath().toString()).contains("code");
    }

    @Test
    void requiresAConditionCode() {
        assertThat(validator.validate(diagnostic(null)))
                .extracting(violation -> violation.getPropertyPath().toString()).contains("code");
    }

    private DeviceError diagnostic(String code) {
        return new DeviceError().code(code).component("SOIL_SENSOR_A")
                .severity(DeviceError.SeverityEnum.CRITICAL).message("Sensor reading failed.");
    }
}
