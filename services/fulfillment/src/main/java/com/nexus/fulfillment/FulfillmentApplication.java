package com.nexus.fulfillment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class FulfillmentApplication {

    public static void main(String[] args) {
        SpringApplication.run(FulfillmentApplication.class, args);
    }
}
