package com.coding;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;


@Configuration
public class KafkaConfig {

    public static final String PRODUCT_CREATED_EVENT_TOPIC = "product-created-event-topic";

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String boostrapServer;
    @Value("${spring.kafka.producer.acks}")
    private String acks;
    @Value("${spring.kafka.producer.retries}")
    private int retries;
    @Value("${spring.kafka.producer.properties.request.enable.idempotence}")
    private boolean enableIdempotence;


    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        return props;
    }

    @Bean
    public DefaultKafkaProducerFactory<String, ProductCreatedEvent> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String,ProductCreatedEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(PRODUCT_CREATED_EVENT_TOPIC)

                .partitions(3)
                .replicas(1)
                .configs(Map.of("min.insync.replicas","1"))
                .build();
    }
}
