package com.github.hsoj48.kafka.annotation;

/**
 * Annotation for defining information for an object that will be received or sent
 * via JSON over a kafka topic.
 *
 * Typically used in conjunction with JsonSerializer/JsonDeserializer.
 */
public @interface KafkaType {

    /**
     * The label to be assigned to this type when sent to or read from a kafka topic
     */
    String value() default "";

}
