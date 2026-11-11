package com.example.agent;

import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.common.KafkaException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public ResponseEntity<String> receiveWebhook(@RequestBody ReviewRequest request) {
        try {
            kafkaTemplate.send(TOPIC, request.pullRequestId(), request);
            return ResponseEntity.accepted().body("Webhook event queued for processing.");
        } catch (KafkaException exception) {
            log.warn("Failed to queue webhook for pull request {}", request.pullRequestId(), exception);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("Kafka is unavailable; webhook was not queued.");
        }
    }
}
