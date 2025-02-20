package com.coding.product.rest;

import com.coding.product.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/products")
public class ProductController {

    private final Logger log = LoggerFactory.getLogger(ProductController.class);

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping
    public ResponseEntity<String> createProduct(@RequestBody CreateProductRestModel product) {
        String productId = productService.createProduct(product);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(productId);
    }

    @PostMapping("/sync")
    public ResponseEntity<?> createProductSynchronously(@RequestBody CreateProductRestModel product) {
        try {
            String productId = productService.createProductSynchronously(product);
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(productId);
        } catch (Exception e) {
            log.error(e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorMessage(LocalDateTime.now(),
                            e.getMessage(),
                            "/products")
                    );
        }
    }
}
