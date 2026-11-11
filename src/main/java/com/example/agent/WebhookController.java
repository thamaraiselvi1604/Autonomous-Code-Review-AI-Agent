package com.example.agent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/webhooks")
public class WebhookController {

    private static final Logger log = LoggerFactory.getLogger(WebhookController.class);
    private final KafkaTemplate<String, ReviewRequest> kafkaTemplate;
    private static final String TOPIC = "code-review-requests";

    public WebhookController(KafkaTemplate<String, ReviewRequest> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/github")
    public ResponseEntity<String> receiveWebhook(@RequestBody(required = false) ReviewRequest request) {
        if (request == null || request.repository() == null || request.repository().isBlank()
                || request.pullRequestId() == null || request.pullRequestId().isBlank()
                || request.patchDiff() == null || request.patchDiff().isBlank()) {
            return ResponseEntity.badRequest()
                    .body("Invalid webhook payload. Expected JSON with repository, pullRequestId, and patchDiff.");
        }

        try {
            kafkaTemplate.send(TOPIC, request.pullRequestId(), request).get(5, TimeUnit.SECONDS);
            return ResponseEntity.accepted().body("Webhook event queued for processing.");
        } catch (org.apache.kafka.common.KafkaException | KafkaException | ExecutionException | TimeoutException e) {
            log.warn("Failed to queue webhook for pull request {}", request.pullRequestId(), e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("Kafka is unavailable; webhook was not queued.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while queuing webhook for pull request {}", request.pullRequestId(), e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("Kafka queueing was interrupted; webhook was not queued.");
        }
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<String> handleMalformedJson(HttpMessageNotReadableException exception) {
        log.warn("Malformed webhook payload received", exception);
        return ResponseEntity.badRequest()
                .body("Invalid JSON payload. Ensure repository, pullRequestId, and patchDiff are valid strings.");
    }
}
