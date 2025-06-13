package com.getTransactions;

import java.sql.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.util.Optional;

public class GetUserFunction {
    @FunctionName("getUser")
    public HttpResponseMessage run(
        @HttpTrigger(name = "req", methods = {HttpMethod.GET}, authLevel = AuthorizationLevel.ANONYMOUS, route = "user")
        HttpRequestMessage<Optional<String>> request,
        final ExecutionContext context
    ) {
        String userId = request.getQueryParameters().get("id");
        if (userId == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Missing user ID").build();
        }

        String connectionUrl = "jdbc:sqlserver://a3sd.database.windows.net:1433;database=transactions;user=DBAdmin@a3sd;password=X7F!*Hmf>Vup&ZB;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";

        try (
            Connection conn = DriverManager.getConnection(connectionUrl);
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM users WHERE user_id = ?");
        ) {
            stmt.setInt(1, Integer.parseInt(userId));
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                String name = rs.getString("full_name");
                String email = rs.getString("email");
                return request.createResponseBuilder(HttpStatus.OK)
                    .body(String.format("{\"id\": %s, \"name\": \"%s\", \"email\": \"%s\"}", userId, name, email))
                    .header("Content-Type", "application/json")
                    .build();
            } else {
                return request.createResponseBuilder(HttpStatus.NOT_FOUND).body("User not found").build();
            }
        } catch (Exception e) {
            context.getLogger().severe("DB error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Database error").build();
        }
    }
}
