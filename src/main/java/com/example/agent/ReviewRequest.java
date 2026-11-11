package com.example.agent;

public record ReviewRequest(String repository, String pullRequestId, String patchDiff) {}
