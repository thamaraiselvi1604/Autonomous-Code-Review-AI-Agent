package com.example.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ComplianceWorker {

    private static final Logger log = LoggerFactory.getLogger(ComplianceWorker.class);
    private final ReviewerAgent reviewerAgent;

    public ComplianceWorker(ReviewerAgent reviewerAgent) {
        this.reviewerAgent = reviewerAgent;
    }

    @KafkaListener(topics = "code-review-requests", groupId = "compliance-agent-group")
    public void processReviewRequest(ReviewRequest request) {
        log.info("Processing asynchronous compliance analysis for PR #{} inside repository: {}", 
                request.pullRequestId(), request.repository());

        try {
            // Trigger the AI prompt abstraction via LangChain4j service Layer
            String complianceFeedback = reviewerAgent.analyzePatch(request.patchDiff());

            log.info("--- AI AGENT COMPLIANCE ANALYSIS COMPLETED ---");
            log.info("\n{}", complianceFeedback);
            log.info("-----------------------------------------------");
            
            // NOTE FOR INTERVIEWS: In production, you would attach an OpenFeign / WebClient 
            // instance here to push 'complianceFeedback' back onto the GitHub REST API Review endpoint.

        } catch (Exception e) {
            log.error("Resilience failure occurred while evaluating AI patch metrics: ", e);
        }
    }
}
