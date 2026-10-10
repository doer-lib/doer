package com.doer.generatedcode;

import static io.restassured.RestAssured.given;

import com.doer.e2e.E2eEnvironment;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * {@link GeneratedCodeTest} on CarWash deployed in the runtime of {@link E2eEnvironment}: the generated service is a
 * CDI bean with real injection and transactions. Each request goes to TaskRunner through
 * {@code POST /api/validation/run-task}.
 */
@ExtendWith(E2eEnvironment.class)
class GeneratedCodeE2E implements GeneratedCodeTest {

    @BeforeEach
    void enableRestLogging() {
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Override
    public String callTaskRunner(String request) {
        return given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/validation/run-task")
                .then()
                .statusCode(200)
                .extract()
                .asString();
    }
}
