package com.doer.e2e;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import org.junit.jupiter.api.Test;

/**
 * The runtime injects beans of every kind a Doer user writes into the generated service, and the generated service
 * calls the doer method of the right bean, through the interceptor where there is one. Each task runs through
 * {@code POST /api/validation/run-task}; the BeanKind* classes write their calls to the trace of the response.
 */
class BeanKindsE2E extends E2eTestBase {

    @Test
    void singleton_bean_should_be_called() {
        assertEquals("""
                {
                    "status": "Bean kind singleton done",
                    "failing": false,
                    "trace": [
                        "BeanKindSingleton.run"
                    ]
                }""", runTask("Bean kind singleton"));
    }

    @Test
    void produced_bean_should_be_called() {
        assertEquals("""
                {
                    "status": "Bean kind produced done",
                    "failing": false,
                    "trace": [
                        "BeanKindProduced.run(BeanKindProducers.produce)"
                    ]
                }""", runTask("Bean kind produced"));
    }

    @Test
    void intercepted_bean_should_be_called_through_interceptor() {
        assertEquals("""
                {
                    "status": "Bean kind intercepted done",
                    "failing": false,
                    "trace": [
                        "BeanKindTracedInterceptor before run",
                        "BeanKindIntercepted.run",
                        "BeanKindTracedInterceptor after run"
                    ]
                }""", runTask("Bean kind intercepted"));
    }

    @Test
    void bean_with_constructor_injection_should_be_called() {
        assertEquals("""
                {
                    "status": "Bean kind constructor done",
                    "failing": false,
                    "trace": [
                        "BeanKindConstructor.run"
                    ]
                }""", runTask("Bean kind constructor"));
    }

    @Test
    void nested_class_bean_should_be_called() {
        assertEquals("""
                {
                    "status": "Bean kind nested done",
                    "failing": false,
                    "trace": [
                        "BeanKinds.Nested.run"
                    ]
                }""", runTask("Bean kind nested"));
    }

    @Test
    void inherited_doer_method_should_be_called_on_subclass_bean() {
        assertEquals("""
                {
                    "status": "Bean kind inherited done",
                    "failing": false,
                    "trace": [
                        "BeanKindBase.run in BeanKindInherited"
                    ]
                }""", runTask("Bean kind inherited"));
    }

    /** Runs a task with the status through TaskRunner; its response pretty-printed. */
    private static String runTask(String status) {
        String response = given()
                .contentType(ContentType.JSON)
                .body("{\"status\": \"" + status + "\"}")
                .when()
                .post("/api/validation/run-task")
                .then()
                .statusCode(200)
                .extract()
                .asString();
        return JsonPath.from(response).prettify();
    }
}
