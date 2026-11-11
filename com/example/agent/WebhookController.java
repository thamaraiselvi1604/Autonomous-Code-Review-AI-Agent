package com.example.agent;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/webhooks")
public class WebhookController {

    private final KafkaTemplate<String, ReviewRequest> kafkaTemplate;
    private static final String TOPIC = "code-review-requests";

    public WebhookController(KafkaTemplate<String, ReviewRequest> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/github")
    public ResponseEntity<String> receiveWebhook(@RequestBody ReviewRequest request) {
        // Stream event immediately into Kafka topic to maintain asynchronous boundary decoupling
        kafkaTemplate.send(TOPIC, request.pullRequestId(), request);
        return ResponseEntity.accepted().body("Webhook event successfully queued for parsing pipeline loop.");
    }
}
