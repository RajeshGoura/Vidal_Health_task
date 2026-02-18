package com.example.webhook_app.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SolutionRequest {
    @JsonProperty("finalQuery")
    private String finalQuery;
    
    public SolutionRequest(String finalQuery) {
        this.finalQuery = finalQuery;
    }
    
    public String getFinalQuery() { return finalQuery; }
    public void setFinalQuery(String finalQuery) { this.finalQuery = finalQuery; }
}