package com.gladys.bridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MqttKafkaBridgeApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqttKafkaBridgeApplication.class, args);
    }
}
