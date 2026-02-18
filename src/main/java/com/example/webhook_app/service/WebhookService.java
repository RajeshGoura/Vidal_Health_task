package com.example.webhook_app.service;

import com.example.webhook_app.model.WebhookRequest;
import com.example.webhook_app.model.WebhookResponse;
import com.example.webhook_app.model.SolutionRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import jakarta.annotation.PostConstruct;

@Service
public class WebhookService {
    
    @Autowired
    private RestTemplate restTemplate;
    
    private static final String GENERATE_URL = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";
    private static final String SUBMIT_URL = "https://bfhldevapigw.healthrx.co.in/hiring/testWebhook/JAVA";
    
    @PostConstruct
    public void init() {
        try {
            // Step 1: Generate webhook
            WebhookRequest request = new WebhookRequest("John Doe", "REG12347", "john@example.com");
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            
            HttpEntity<WebhookRequest> entity = new HttpEntity<>(request, headers);
            
            ResponseEntity<WebhookResponse> response = restTemplate.exchange(
                GENERATE_URL,
                HttpMethod.POST,
                entity,
                WebhookResponse.class
            );
            
            WebhookResponse webhookResponse = response.getBody();
            
            if (webhookResponse != null) {
                // Step 2: Get SQL query based on regNo last digit
                String regNo = request.getRegNo(); // "REG12347"
                int lastDigit = Integer.parseInt(regNo.substring(regNo.length() - 1));
                
                String finalQuery;
                if (lastDigit % 2 == 1) { // Odd - Question 1
                    finalQuery = getOddRegNoQuery();
                } else { // Even - Question 2
                    finalQuery = getEvenRegNoQuery();
                }
                
                // Step 3: Submit solution
                submitSolution(webhookResponse.getAccessToken(), finalQuery);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private String getOddRegNoQuery() {
        // Question 1: Highest salaried employee per department (excluding 1st of month payments)
        return """
            WITH EmployeeSalaries AS (
                SELECT 
                    e.EMP_ID,
                    e.FIRST_NAME,
                    e.LAST_NAME,
                    e.DOB,
                    e.DEPARTMENT,
                    SUM(p.AMOUNT) as TOTAL_SALARY
                FROM EMPLOYEE e
                JOIN PAYMENTS p ON e.EMP_ID = p.EMP_ID
                WHERE EXTRACT(DAY FROM p.PAYMENT_TIME) != 1
                GROUP BY e.EMP_ID, e.FIRST_NAME, e.LAST_NAME, e.DOB, e.DEPARTMENT
            ),
            RankedSalaries AS (
                SELECT 
                    d.DEPARTMENT_NAME,
                    es.TOTAL_SALARY as SALARY,
                    CONCAT(es.FIRST_NAME, ' ', es.LAST_NAME) as EMPLOYEE_NAME,
                    EXTRACT(YEAR FROM AGE(CURRENT_DATE, es.DOB)) as AGE,
                    ROW_NUMBER() OVER (PARTITION BY d.DEPARTMENT_ID ORDER BY es.TOTAL_SALARY DESC) as rn
                FROM EmployeeSalaries es
                JOIN DEPARTMENT d ON es.DEPARTMENT = d.DEPARTMENT_ID
            )
            SELECT DEPARTMENT_NAME, SALARY, EMPLOYEE_NAME, AGE
            FROM RankedSalaries
            WHERE rn = 1
            ORDER BY DEPARTMENT_NAME;
            """;
    }
    
    private String getEvenRegNoQuery() {
        // Question 2: Average age and employee list for salaries > 70000
        return """
            WITH HighEarners AS (
                SELECT 
                    e.EMP_ID,
                    e.FIRST_NAME,
                    e.LAST_NAME,
                    e.DOB,
                    d.DEPARTMENT_ID,
                    d.DEPARTMENT_NAME,
                    p.AMOUNT
                FROM EMPLOYEE e
                JOIN PAYMENTS p ON e.EMP_ID = p.EMP_ID
                JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID
                WHERE p.AMOUNT > 70000
            )
            SELECT 
                DEPARTMENT_NAME,
                ROUND(AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, DOB))), 2) as AVERAGE_AGE,
                (
                    SELECT STRING_AGG(CONCAT(FIRST_NAME, ' ', LAST_NAME), ', ')
                    FROM (
                        SELECT DISTINCT FIRST_NAME, LAST_NAME
                        FROM HighEarners h2
                        WHERE h2.DEPARTMENT_ID = h1.DEPARTMENT_ID
                        LIMIT 10
                    ) limited
                ) as EMPLOYEE_LIST
            FROM HighEarners h1
            GROUP BY DEPARTMENT_ID, DEPARTMENT_NAME
            ORDER BY DEPARTMENT_ID DESC;
            """;
    }
    
    private void submitSolution(String accessToken, String finalQuery) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("Authorization", accessToken); // Using the token as is (without "Bearer" prefix as per spec)
            
            SolutionRequest solutionRequest = new SolutionRequest(finalQuery);
            HttpEntity<SolutionRequest> entity = new HttpEntity<>(solutionRequest, headers);
            
            ResponseEntity<String> response = restTemplate.exchange(
                SUBMIT_URL,
                HttpMethod.POST,
                entity,
                String.class
            );
            
            System.out.println("Submission Response: " + response.getBody());
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}