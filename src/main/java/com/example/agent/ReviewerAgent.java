package com.example.agent;

import dev.langchain4j.service.SystemMessage;
import dev.langchain4j.service.UserMessage;
import dev.langchain4j.service.spring.AiService;

@AiService
public interface ReviewerAgent {

    @SystemMessage("""
        Act as a Principal Java and Cloud Solutions Architect.
        Analyze the incoming Git patch string variations for code smells, resource leaks, or security flaws.
        Provide a concise code review comment summarizing code flaws or structural improvements.
        Focus heavily on missing unit coverage, NullPointerExceptions, and optimal thread usage.
        Keep the output clear and technical.
        """)
    String analyzePatch(@UserMessage String patchDiff);
}
