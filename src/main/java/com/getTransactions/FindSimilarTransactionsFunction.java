package com.getTransactions;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;

import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class FindSimilarTransactionsFunction {
    @FunctionName("findSimilarTransactions")
    public HttpResponseMessage run(
        @HttpTrigger(name = "req", methods = {HttpMethod.GET}, authLevel = AuthorizationLevel.ANONYMOUS, route = "similar-transactions")
        HttpRequestMessage<Optional<String>> request,
        final ExecutionContext context
    ) {
        String datetimeParam = request.getQueryParameters().get("datetime");
        String amountParam = request.getQueryParameters().get("amount");

        if (datetimeParam == null || amountParam == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                .body("Please provide both 'datetime' and 'amount' query parameters.")
                .build();
        }

        try {
            LocalDateTime inputTime = LocalDateTime.parse(datetimeParam);
            double inputAmount = Double.parseDouble(amountParam);

            // Define tolerance
            LocalDateTime timeLower = inputTime.minusMinutes(5);
            LocalDateTime timeUpper = inputTime.plusMinutes(5);
            double amountLower = inputAmount - 10;
            double amountUpper = inputAmount + 10;

            String connectionUrl = "jdbc:sqlserver://a3sd.database.windows.net:1433;database=transactions;user=DBAdmin@a3sd;password=X7F!*Hmf>Vup&ZB;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";

            String query = "SELECT * FROM transactions WHERE transaction_date BETWEEN ? AND ? AND amount BETWEEN ? AND ?";

            List<Map<String, Object>> results = new ArrayList<>();

            try (
                Connection conn = DriverManager.getConnection(connectionUrl);
                PreparedStatement stmt = conn.prepareStatement(query)
            ) {
                stmt.setTimestamp(1, Timestamp.valueOf(timeLower));
                stmt.setTimestamp(2, Timestamp.valueOf(timeUpper));
                stmt.setDouble(3, amountLower);
                stmt.setDouble(4, amountUpper);

                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("transaction_id", rs.getInt("transaction_id"));
                    row.put("user_id", rs.getInt("user_id"));
                    row.put("transaction_date", rs.getTimestamp("transaction_date").toString());
                    row.put("amount", rs.getDouble("amount"));
                    row.put("description", rs.getString("description"));
                    results.add(row);
                }
            }

            return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(results)
                .build();

        } catch (Exception e) {
            context.getLogger().severe("Error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to process request").build();
        }
    }
}
