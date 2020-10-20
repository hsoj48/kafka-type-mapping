package com.github.hsoj48.kafka.spring.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@Data
@ConfigurationProperties(prefix = "spring.kafka.type-mapping.packages")
public class KafkaTypeMappingConfigProperties {

    /**
     * A comma-delimited list of base packages to include in the scan for candidates.
     * Packages added to this property will also populate the property for 'spring.json.trusted.packages' for deserialization.
     */
    private List<String> include = new ArrayList<>();

    /**
     * A comma-delimited list of base packages to exclude in the scan for candidates.
     * Packages added to this property will be ignored if 'spring.kafka.type-mapping.packages.include' is not populated.
     */
    private List<String> exclude = new ArrayList<>();

}
