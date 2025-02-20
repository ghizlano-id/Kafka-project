package com.coding.product.service;

import com.coding.KafkaConfig;
import com.coding.ProductCreatedEvent;
import com.coding.product.rest.CreateProductRestModel;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class ProductService {

    private final Logger log = LoggerFactory.getLogger(ProductService.class);

    private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    public ProductService(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String createProduct(CreateProductRestModel product) {

        String productId = UUID.randomUUID().toString();
        //TODO : persist product
        var productCreatedEvent = new ProductCreatedEvent(productId,
                product.getTitle(),
                product.getPrice(),
                product.getQuantity());

        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate.send(KafkaConfig.PRODUCT_CREATED_EVENT_TOPIC,
                productId,
                productCreatedEvent);
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("******* Failed : {}", exception.getMessage());
            }else {
                log.info("******* Success :{}", result.getRecordMetadata());
            }
        });

        log.info("******* returning product Id :{}", productId);

        return productId;
    }

    public String createProductSynchronously(CreateProductRestModel product) throws ExecutionException, InterruptedException {
        String productId = UUID.randomUUID().toString();
        //TODO : persist product
        var productCreatedEvent = new ProductCreatedEvent(productId,
                product.getTitle(),
                product.getPrice(),
                product.getQuantity());
        var producerRecord = new ProducerRecord<String,ProductCreatedEvent>(
                KafkaConfig.PRODUCT_CREATED_EVENT_TOPIC,
                productId,
                productCreatedEvent
        );
        producerRecord.headers().add("messageId",UUID.randomUUID().toString().getBytes());

        log.info("******* Start publishing productCreatedEvent to Kafka ");
        var result = kafkaTemplate.send(producerRecord).get();
        /*SendResult<String, ProductCreatedEvent> result = kafkaTemplate.send(KafkaConfig.PRODUCT_CREATED_EVENT_TOPIC,
                productId,
                productCreatedEvent).get();*/

        log.info("Topic : {}", result.getRecordMetadata().topic());
        log.info("Partition : {}", result.getRecordMetadata().partition());
        log.info("Offset : {}", result.getRecordMetadata().offset());
        log.info("******* End publishing productCreatedEvent to Kafka ");
        return productId;
    }
}
