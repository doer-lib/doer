# Doer

[![maven](https://badges.mvnrepository.com/badge/com.java-doer/doer/badge.svg?label=maven)](https://mvnrepository.com/artifact/com.java-doer/doer)

Doer is a Java library designed for building fault-tolerant, cluster-ready backend applications using annotations.

It simplifies distributed task execution, automatic retries, and state transitions to help build reliable workflows.

## Features

- Annotation-based API
- Execution in distributed environments
- Automatic retries and failover
- Coordinated updates between Doer tasks and external APIs

## Usage

Requires Java 17+. Generated code depends on Jakarta EE 10+ (CDI, JTA, JSON-P) and PostgreSQL.

```xml
<dependency>
    <groupId>com.java-doer</groupId>
    <artifactId>doer</artifactId>
    <version>0.0.16</version>
</dependency>
```

Version 1.0 is in development on `main`, and this README describes it. It renames the public API (for example `@OnException` becomes `@RetryPolicy`); see [api-v1-design.md](api-v1-design.md#migration).

### Annotation processing since JDK 23

Starting with JDK 23, annotation processing must be explicitly enabled.
You can enable it for all processors using -Dmaven.compiler.proc=full, or configure the Doer processor explicitly:

```xml
<plugin>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <annotationProcessorPaths>
            <path>
                <groupId>com.java-doer</groupId>
                <artifactId>doer</artifactId>
                <version>0.0.16</version>
            </path>
        </annotationProcessorPaths>
    </configuration>
</plugin>
```

### What the library provides

Define workflows in your backend using annotations. The annotation processor generates the implementation of `com.doer.DoerService`, which your backend can inject and use as a regular dependency. Beans with doer methods may inject `DoerService` too: the generated service injects providers of the beans and looks them up on first use, so there is no circular reference to resolve, in Spring as well.

```java
@ApplicationScoped
public class DoerLifecycle {
    @Inject
    DoerService doerService;

    void onStart(@Observes Startup event) {
        doerService.start(true);
    }

    void onStop(@Observes Shutdown event) {
        doerService.stop();
    }
}
```

## Example

The following code illustrates an order-processing workflow from the [Doer Tutorial](https://github.com/doer-lib/doertutorial).

```java
@AcceptStatus(GOODS_RESERVED)
@RetryPolicy(interval = "5s", duration = "10s", fallbackStatus = PAYMENT_FAILED)
public void payOrder(Task task, Order order) {
    Check check = bank.processPayment(order);
    order.setPaymentTransactionId(check.transactionId());
    task.setStatus(ORDER_PAID);
}

@AcceptStatus(PAYMENT_FAILED)
public void reportNoPaymentForOrder(Task task, Order order) {
    order.setRejectReason("Payment not processed.");
    task.setStatus(REJECTED_NO_PAYMENT);
}

@AcceptStatus(ORDER_PAID)
@RetryPolicy(interval = "5s", duration = "5s", fallbackStatus = ORDER_NOT_SHIPPED)
public void shipOrder(Task task, Order order) {
    TrackId trackId = warehouse.shipTheOrder(order);
    order.setDeliveryTrackingId(trackId.token());
    task.setStatus(ORDER_SHIPPED);
}
```

The following diagram illustrates the state transitions and error-handling mechanisms in this workflow:

![intro.svg](intro.svg)

## Tutorials

Video tutorials are available on [YouTube](https://www.youtube.com/@DoerLib):

1. [Episode 01: The Core Concept Behind the Doer](https://github.com/doer-lib/doertutorial/wiki/Episode-01:-The-Core-Concept-Behind-the-Doer)
2. [Episode 02: Doer Methods - Implementing Business Operations with Doer](https://github.com/doer-lib/doertutorial/wiki/Episode-02.-Doer-Methods:-Implementing-Business-Operations-with-Doer)
3. [Episode 03: How to Read a Doer Diagram](https://github.com/doer-lib/doertutorial/wiki/Episode-03:-How-to-Read-a-Doer-Diagram)

## Tools

- [Doer Diagram Tool](https://www.java-doer.com/doer-diagram.html): visualize your Doer workflows

## Development

Contributions and feedback are welcome! Feel free to open an issue or submit a pull request.
