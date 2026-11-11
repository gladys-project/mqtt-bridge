package com.gladys.bridge.mapping;

import java.util.List;

import com.gladys.bridge.message.DeviceError;
import com.gladys.bridge.message.KafkaRawTelemetry;
import com.gladys.bridge.message.MqttTelemetryMessage;
import com.gladys.bridge.message.TelemetryPayload;
import org.mapstruct.AfterMapping;
import org.mapstruct.BeanMapping;
import org.mapstruct.InjectionStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.ReportingPolicy;

/** Maps one complete observation without splitting or interpreting its contents. */
@Mapper(componentModel = MappingConstants.ComponentModel.SPRING,
        injectionStrategy = InjectionStrategy.CONSTRUCTOR,
        unmappedSourcePolicy = ReportingPolicy.ERROR,
        unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MqttToKafkaMapper {
    KafkaRawTelemetry toKafka(MqttTelemetryMessage source);

    List<DeviceError> copyErrors(List<DeviceError> source);

    DeviceError copyError(DeviceError source);

    @BeanMapping(ignoreUnmappedSourceProperties = "additionalProperties")
    @Mapping(target = "additionalProperties", ignore = true)
    TelemetryPayload copyPayload(TelemetryPayload source);

    // Generated extensible DTOs have an any-setter rather than a map setter.
    @AfterMapping
    default void copyMeasurements(TelemetryPayload source, @MappingTarget TelemetryPayload target) {
        if (source.getAdditionalProperties() != null) {
            source.getAdditionalProperties().forEach(target::putAdditionalProperty);
        }
    }
}
