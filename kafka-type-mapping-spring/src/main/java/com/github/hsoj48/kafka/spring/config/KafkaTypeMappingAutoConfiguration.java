package com.github.hsoj48.kafka.spring.config;

import com.github.hsoj48.kafka.annotation.KafkaType;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass(KafkaTemplate.class)
@AutoConfigureBefore(KafkaAutoConfiguration.class)
@EnableConfigurationProperties({KafkaProperties.class, KafkaTypeMappingConfigProperties.class})
@ConditionalOnProperty(name = "spring.kafka.type-mapping.packages.include")
public class KafkaTypeMappingAutoConfiguration {

    @Autowired private KafkaProperties kafkaProperties;
    @Autowired private KafkaTypeMappingConfigProperties kafkaTypeMappingProperties;

    @Bean
    @ConditionalOnMissingBean(ConsumerFactory.class)
    public ConsumerFactory<?, ?> annotationAddedKafkaConsumerFactory(ObjectProvider<DefaultKafkaConsumerFactoryCustomizer> customizers) {
        Map<String, Object> consumerProperties = this.kafkaProperties.buildConsumerProperties();
        addTrustedPackages(consumerProperties);
        addAnnotationBasedTypes(consumerProperties);

        DefaultKafkaConsumerFactory<Object, Object> factory = new DefaultKafkaConsumerFactory<>(consumerProperties);
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean(ProducerFactory.class)
    public ProducerFactory<?, ?> annotationAddedKafkaProducerFactory(ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers) {
        Map<String, Object> producerProperties = this.kafkaProperties.buildProducerProperties();
        addAnnotationBasedTypes(producerProperties);

        DefaultKafkaProducerFactory<?, ?> factory = new DefaultKafkaProducerFactory<>(producerProperties);
        String transactionIdPrefix = this.kafkaProperties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    private void addTrustedPackages(Map<String, Object> configs) {
        if (configs.containsKey(JsonDeserializer.TRUSTED_PACKAGES) && kafkaTypeMappingProperties.getInclude() != null) {
            configs.put(JsonDeserializer.TRUSTED_PACKAGES, configs.get(JsonDeserializer.TRUSTED_PACKAGES) + "," + String.join(",", kafkaTypeMappingProperties.getInclude()));
        } else if (kafkaTypeMappingProperties.getInclude() != null) {
            configs.put(JsonDeserializer.TRUSTED_PACKAGES, String.join(",", kafkaTypeMappingProperties.getInclude()));
        }
    }

    private void addAnnotationBasedTypes(Map<String, Object> configs) {
        ClassPathScanningCandidateComponentProvider scanner = buildScanner();
        Map<String, Set<String>> mappings = kafkaTypeMappingProperties.getInclude().stream()
                .flatMap(o -> scanner.findCandidateComponents(o).stream())
                .collect(Collectors.groupingBy(this::getKafkaTypeAnnotationValue, Collectors.mapping(BeanDefinition::getBeanClassName, Collectors.toSet())));

        validateDistinctLabels(mappings);

        String additionalMappings = collectMappings(mappings);
        if (configs.containsKey(JsonSerializer.TYPE_MAPPINGS)) {
            configs.put(JsonSerializer.TYPE_MAPPINGS, configs.get(JsonSerializer.TYPE_MAPPINGS) + "," + additionalMappings);
        } else {
            configs.put(JsonSerializer.TYPE_MAPPINGS, additionalMappings);
        }
    }

    private ClassPathScanningCandidateComponentProvider buildScanner() {
        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(KafkaType.class));

        if (kafkaTypeMappingProperties.getExclude() != null && !kafkaTypeMappingProperties.getExclude().isEmpty()) {
            scanner.addExcludeFilter(new PackagePrefixMatchingFilter(kafkaTypeMappingProperties.getExclude()));
        }

        return scanner;
    }

    private String collectMappings(Map<String, Set<String>> mappings) {
        List<String> definitions = new ArrayList<>(mappings.size());
        mappings.forEach((label, classNames) -> definitions.add(label + ":" + classNames.iterator().next()));
        return String.join(",", definitions);
    }

    private void validateDistinctLabels(Map<String, Set<String>> mappings) {
        mappings.forEach((s, classNames) -> {
            if (classNames.size() != 1) {
                throw new IllegalArgumentException("KafkaType annotation must have a unique value; multiple classes found for label [" + s + "]");
            }
        });
    }

    private String getKafkaTypeAnnotationValue(BeanDefinition def) {
        KafkaType annotation = toClass(def).getAnnotation(KafkaType.class);

        if (!"".equals(annotation.value())) {
            return annotation.value();
        } else {
            throw new IllegalArgumentException("KafkaType annotation must have a specified value");
        }
    }

    private Class<?> toClass(BeanDefinition def) {
        try {
            return Class.forName(def.getBeanClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    static class PackagePrefixMatchingFilter implements TypeFilter {

        private final List<String> packagePrefixes;

        PackagePrefixMatchingFilter(List<String> packagePrefixes) {
            this.packagePrefixes = packagePrefixes;
        }

        @Override
        public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory) throws IOException {
            return packagePrefixes.stream().anyMatch(o -> metadataReader.getClassMetadata().getClassName().startsWith(o));
        }
    }

}
