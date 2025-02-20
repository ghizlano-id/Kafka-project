package com.coding.handler;

import com.coding.ProductCreatedEvent;
import com.coding.error.NotRetryableException;
import com.coding.error.RetryableException;
import com.coding.io.ProcessedEventEntity;
import com.coding.io.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.List;

@Component
@KafkaListener(topics = "product-created-event-topic")
public class ProductCreatedEventHandler {

    private final Logger log = LoggerFactory.getLogger(ProductCreatedEventHandler.class);

    private final RestTemplate restTemplate;
    private final ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate,
                                      ProcessedEventRepository processedEventRepository) {
        this.restTemplate = restTemplate;
        this.processedEventRepository = processedEventRepository;
    }

    //@KafkaListener(topics = "product-created-event-topic")
    @KafkaHandler
    @Transactional
    public void handle(@Payload ProductCreatedEvent product,
                       @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
        log.info("***** Received new event: {}", product.getTitle());

        // Check if this message was already processed before
        ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);
        if (existingRecord != null) {
            log.info("***** Found a duplicate message id: {}", existingRecord.getMessageId());
            return;
        }

        try {
            var response = restTemplate.getForEntity("http://localhost:8083/response/200", String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("***** Received response from remote server: {}", response.getBody());
            }
        } catch (ResourceAccessException ex) {
            log.error(ex.getMessage());
            throw new RetryableException(ex);
        } catch (HttpServerErrorException ex) {
            log.error(ex.getMessage());
            throw new NotRetryableException(ex);
        } catch (Exception ex) {
            log.error(ex.getMessage());
            throw new NotRetryableException(ex);
        }

        // Save unique message id in DB
        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId,
                    product.getProductId()));
        }catch(DataIntegrityViolationException ex) {
            log.error(ex.getMessage());
            throw new NotRetryableException(ex);
        }
    }
}
