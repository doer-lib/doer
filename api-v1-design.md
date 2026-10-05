# Public API names for v1

Status: proposal

v1 is the last chance to rename public types without a deprecation cycle. This document lists every public type in `com.doer`, the package users code against, points out where the current names are inconsistent, and proposes one naming scheme for all of them.

## Current public types

| Kind | Type | Members |
|---|---|---|
| Annotation | `AcceptStatus` | `value`, `delay` |
| Annotation | `AcceptStatuses` | `value` (repeatable container) |
| Annotation | `OnException` | `retry`, `setStatus` |
| Annotation | `DoerConcurrency` | `value` |
| Annotation | `DoerLoader` | — |
| Annotation | `DoerUnloader` | — |
| Annotation | `DoerExtraJson` | — |
| Interface | `DoerTaskConsumer` | `apply(Task)` |
| Interface | `DoerUpdater<T>` | `applyUpdate(Task, T)` |
| Interface | `ConcurrencyDomain` | `getName`, `getValue`, `getStatuses`, `getDelay`, `getRetryDelay` |
| Class | `DoerService` | entry point, abstract; generated subclass `com.doer.generated._GeneratedDoerService` |
| Class | `Task` | task row |
| Exception | `TaskNotFoundException` | `getTaskId()` |
| Exception | `TaskInProgressException` | `getTaskId()` |
| Exception | `OptimisticLockException` | no `getTaskId()`; package-private `taskId` is never set |

## Problems

1. **The `Doer` prefix is used at random.** `DoerConcurrency`, `DoerLoader`, `DoerUnloader` and `DoerExtraJson` have it; `AcceptStatus` and `OnException`, the two most used annotations, do not. Among the exceptions, none have it. The package `com.doer` already provides the namespace.
2. **The two updater interfaces do not look like a pair.** `DoerTaskConsumer.apply(Task)` and `DoerUpdater<T>.applyUpdate(Task, T)` are passed to the same method (`facilitateCoordinatedUpdate`), but have unrelated names and different method names.
3. **"Unloader" is not a common word** for saving data, and the processor's own error messages already call the loader `DoerParameterLoader`, a third name for the same thing.
4. **`DoerExtraJson` names the storage format, not the purpose.** The method is called only when a doer method throws, receives the exception, and adds details about it to `task_logs.extra_json`.
5. **`OnException` has a mini-language and a required field that is sometimes ignored.** `retry = "every 5s during 10s"` mixes two durations in one string, while `AcceptStatus.delay` is a plain duration (`"5s"`). `setStatus` is required, but without `during ...` the task retries forever and the status is never used.
6. **`OptimisticLockException` has the same simple name as `jakarta.persistence.OptimisticLockException`.** Code that uses JPA and Doer together has to qualify one of them. Unlike the other two task exceptions, it also does not expose the task id.
7. **`ConcurrencyDomain` is public but unreachable.** No public method returns it; `DoerService` keeps the domains in a private field. Its `getValue()` is a leftover from the annotation's `value()`.
8. **`DoerConcurrency` cannot group methods from different classes.** The domain name is derived from the annotated element: the class name for a class, `Class.method` for a method. v1 adds a separate annotation that names the domain (see below).

## Naming rules

1. **`Doer` prefix only for the library entry point:** `DoerService`.
2. **`Task` prefix for types about one task:** `Task`, the task exceptions, the updater interfaces, the loader and saver annotations.
3. **Annotations have no `Doer` prefix and are named by their role:** a noun for what the method *is* or for a setting it has (`@TaskDataLoader`, `@RetryPolicy`, `@ConcurrencyGroup`, `@ConcurrencyLimit`), a verb phrase for what it *does* (`@AcceptStatus`).
4. **Every duration in an annotation is a plain duration string** in the format `AcceptStatus.delay` already uses: `"5s"`, `"10 min"`, `"2h"`, `"1 day"`. No mini-languages.
5. **A status in an annotation is either `value` (the main subject of the annotation) or `<role>Status`.**
6. **The concept "data loaded for a task" is called *task data* everywhere, or *task and data* when both the task and its data are meant:** in the loader and saver annotations (`TaskDataLoader`, `TaskDataSaver`), in the updater interface (`TaskAndDataUpdater`), and in the `data` parameter that `DoerUpdater` already has.

## Proposed names

| Current | v1 | Why |
|---|---|---|
| `@AcceptStatus(value, delay)` | `@AcceptStatus(value, delay)` | Already fits the rules. |
| `@AcceptStatuses` | `@AcceptStatuses` | Standard Java container name for a repeatable annotation. |
| `@OnException(retry, setStatus)` | `@RetryPolicy(interval, duration, fallbackStatus)` | Rules 3, 4 and 5; see below. |
| `@DoerConcurrency(value)` | `@ConcurrencyLimit(value)` and `@ConcurrencyGroup(value)` | Rule 3; the limit and the domain name are separate settings; adds named domains; see below. |
| `@DoerLoader` | `@TaskDataLoader` | Rules 3 and 6. |
| `@DoerUnloader` | `@TaskDataSaver` | Rules 3 and 6; "save" is the usual word. |
| `@DoerExtraJson` | `@ExceptionDescriber` | Names the role of the method (problem 4); see below. |
| `DoerTaskConsumer.apply(Task)` | `TaskUpdater.update(Task)` | Pairs with `TaskAndDataUpdater`. |
| `DoerUpdater<T>.applyUpdate(Task, T)` | `TaskAndDataUpdater<T>.update(Task, T)` | Rule 6; updates both the task and its data; same method name as its pair. |
| `ConcurrencyDomain` | package-private (or keep public with `getValue` → `getLimit`) | See below. |
| `DoerService` | `DoerService` | Rule 1. |
| `Task` | `Task` | — |
| `TaskNotFoundException` | `TaskNotFoundException` | — |
| `TaskInProgressException` | `TaskInProgressException` | — |
| `OptimisticLockException` | `TaskVersionConflictException`, with `getTaskId()` | No clash with JPA (problem 6); says what conflicted. |

`com.doer.processor` is not user API. `DoerProcessor` must stay public so that the compiler can discover it; `SetStatusFinder` is used only by `DoerProcessor` and can become package-private.

The generated class `com.doer.generated._GeneratedDoerService` is not something users name in their code (they inject `DoerService`), so it keeps its name. The leading underscore keeps it from colliding with user classes.

### Before and after

```java
// 0.x
@DoerConcurrency(3)
@AcceptStatus(GOODS_RESERVED)
@OnException(retry = "every 5s during 10s", setStatus = PAYMENT_FAILED)
public void payOrder(Task task, Order order) { ... }

@DoerLoader
public Order loadOrder(Task task) { ... }

@DoerUnloader
public void storeOrder(Task task, Order order) { ... }

@DoerExtraJson
public void bankError(Task task, BankException e, JsonObjectBuilder json) { ... }
```

```java
// v1
@ConcurrencyLimit(3)
@AcceptStatus(GOODS_RESERVED)
@RetryPolicy(interval = "5s", duration = "10s", fallbackStatus = PAYMENT_FAILED)
public void payOrder(Task task, Order order) { ... }

@TaskDataLoader
public Order loadOrder(Task task) { ... }

@TaskDataSaver
public void saveOrder(Task task, Order order) { ... }

@ExceptionDescriber
public void describeBankError(Task task, BankException e, JsonObjectBuilder json) { ... }
```

## `@ExceptionDescriber` (was `@DoerExtraJson`)

The annotated method is called when a doer method throws an exception of the type of its second parameter (or a subclass). It reads the task and the exception and adds fields to the `JsonObjectBuilder`, which Doer writes to `task_logs.extra_json`. Doer also calls describers for the exception's cause and suppressed exceptions, recursively; their details go into nested `cause` and `suppressed` objects:

```java
public void describeBankError(Task task, BankException e, JsonObjectBuilder json) {
    json.add("bankCode", e.getCode());
}
```

Like `@TaskDataLoader` and `@TaskDataSaver`, the annotation marks a helper method that Doer calls with task-related arguments, so its name should be an agent noun: *what the method is*, not what the result is. `ExceptionDetails` names the result.

| Option | Use site | Notes |
|---|---|---|
| **`@ExceptionDescriber`** (recommended) | `describeBankError(...)` | Says exactly what the method does with the exception. Short, and the matching method name `describeX` reads like `loadX` / `saveX`. |
| `@ExceptionRecorder` | `recordBankError(...)` | Familiar word, and the task log is a record of what happened. But it suggests that the method records the exception itself (stores it, decides whether to record it). In fact Doer always records the exception, and the method only adds details. Same problem as `ExceptionLogger`, though milder. |
| `@ExceptionDetailsWriter` | `writeBankErrorDetails(...)` | Most literal; slightly long, and `Writer` hints at `java.io.Writer`. |
| `@ExceptionLogEnricher` | `enrichBankErrorLog(...)` | Says that it adds to the task log rather than creating it; "enricher" is a less familiar word. |
| `@TaskLogEnricher` | `enrichLog(...)` | Groups it with the task log instead of the exception; hides that it is called only on exceptions. |
| `@ExceptionDetailsCollector` | `collectBankErrorDetails(...)` | Fits the builder style, but "collector" suggests `java.util.stream.Collector`. |

Rejected: `ExceptionLogger` (confused with logging frameworks and suggests the method writes the log itself), `ExceptionSerializer` (suggests full serialization, as in Jackson), and anything with `Json` in the name (names the storage format, problem 4).

## `@RetryPolicy` (was `@OnException`)

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface RetryPolicy {
    /** Delay between attempts, e.g. "5s". Required. */
    String interval();

    /** How long to keep retrying; "" means forever. */
    String duration() default "";

    /** Status to set when duration has passed; "" means null. Requires duration. */
    String fallbackStatus() default "";
}
```

Usage:

```java
@RetryPolicy(interval = "5m", duration = "30m", fallbackStatus = PAYMENT_FAILED)
```

- **Why `RetryPolicy`.** The annotation configures what happens to a failing task: how often it is retried, for how long, and which status it gets afterwards. `OnException` names only the trigger. Like `ConcurrencyLimit`, it is a noun for a setting of the doer method.
- **Field names.** Inside `@RetryPolicy` the `retry` prefix is redundant, so the fields are just `interval` and `duration`. Both use the plain duration format of `AcceptStatus.delay`, which replaces the `"every 5s during 10s"` mini-language.
- **`interval` is required.** A retry policy without an interval says nothing, and a default would hide the choice.
- **Compile-time check:** `fallbackStatus` without `duration` is an error, because it would never be used. Today `setStatus` is required even when there is no `during`, and then it is silently ignored.
- **`duration` without `fallbackStatus`** sets the status to `null` when the duration has passed, so the task stops being processed. This is the same behaviour that v1 will probably use for methods without the annotation (see below), with the interval and duration chosen by the user.
- `fallbackStatus` replaces `setStatus`: it says when the status is used, and `setStatus` reads like a method call.

### Methods without `@RetryPolicy` (probable behaviour change)

In 0.x, a doer method without `@OnException` is retried every 5 minutes forever.

In v1 it will probably be retried for a fixed time only (interval and time to be decided). After that, Doer sets the task status to `null`. No doer method accepts `null`, so the task stops being processed. Like any status change, this one is written to `task_logs`.

Retrying forever then has to be asked for explicitly. To keep the 0.x behaviour, annotate the method with:

```java
@RetryPolicy(interval = "5m")    // no duration: retry every 5 minutes, forever
```

## `@ConcurrencyGroup`, `@ConcurrencyLimit` and named domains

`@DoerConcurrency` is split into two annotations: one names the concurrency domain, the other sets its limit.

```java
@Target({ ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.CLASS)
public @interface ConcurrencyGroup {
    /** Name of the concurrency domain the annotated method or class runs in. */
    String value();
}

@Target({ ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.CLASS)
public @interface ConcurrencyLimit {
    /** Maximum number of tasks of this domain that run at the same time on one node. */
    int value();
}
```

Usage:

```java
@ConcurrencyGroup("com.example.OrderProcessor")
@ConcurrencyLimit(5)
```

**Why two annotations.** The domain name and the limit are independent settings. Each annotation has a single `value`, so both always use the short form, and the 0.x short form `@DoerConcurrency(5)` becomes `@ConcurrencyLimit(5)`. A method can join a domain with `@ConcurrencyGroup` alone, without repeating the limit declared elsewhere (see "Joining a domain").

**Why `ConcurrencyGroup` and not `ConcurrencyDomain`.** `@ConcurrencyDomain` would collide with the runtime interface of the same name in `com.doer`. "Group" says what the annotation does: methods and classes with the same name are grouped under one limit.

**Why `ConcurrencyLimit` and not `Concurrency`.** The number is a limit. "Limit 5" reads naturally; "concurrency 5" does not say whether 5 is a minimum, a maximum or an exact count.

### Which domain a doer method belongs to

The first rule that applies wins:

1. The method has `@ConcurrencyGroup`: its `value`.
2. The method has `@ConcurrencyLimit`: `<class FQN>.<method>` (as today).
3. The class has `@ConcurrencyGroup`: its `value`.
4. Otherwise: `<class FQN>` (as today).

`@ConcurrencyLimit` sets the limit of the domain that its element (method or class) resolves to. A class-level `@ConcurrencyLimit` therefore does not apply to a method that has its own `@ConcurrencyGroup` or `@ConcurrencyLimit`. A domain for which no `@ConcurrencyLimit` declares a limit gets limit 2 (as today).

Without `@ConcurrencyGroup`, every v1 program gets the same domains as in 0.x.

### Joining a domain

- **Methods and classes with the same `@ConcurrencyGroup` share one domain**: one limit, one set of queues, across all of them.
- **An explicit name may equal a derived name.** `@ConcurrencyGroup("com.example.OrderProcessor")` on a method of another class joins the default domain of class `com.example.OrderProcessor`. This is intended, and it is the reason the example above uses a class FQN.
- **The limit can be declared once.** It is enough to put `@ConcurrencyLimit` on one member of the domain; the others join it with `@ConcurrencyGroup` alone.
- **If the limit is declared in several places, it must be the same.** If two `@ConcurrencyLimit` annotations apply to the same domain with different values, compilation fails and the error lists all of them. Shared constants keep them in sync:

  ```java
  public static final String ORDER_DOMAIN = "com.example.OrderProcessor";
  public static final int ORDER_LIMIT = 5;

  @ConcurrencyGroup(ORDER_DOMAIN)
  @ConcurrencyLimit(ORDER_LIMIT)
  ```

  This is stricter than "the largest value wins", but a mismatch is almost always a mistake, and a silent choice would hide it.
- **Only declared limits are compared.** The default limit 2 applies only when no annotation declares a limit for the domain. So if class `com.example.OrderProcessor` has no annotation and another method joins its domain with `@ConcurrencyGroup("com.example.OrderProcessor")` and `@ConcurrencyLimit(5)`, the shared domain has limit 5.
- **Joining is always explicit.** A method or class without `@ConcurrencyGroup` never joins a named domain.
- `doer.json` and `doer.dot` group methods by the resolved domain name, so the diagram shows which methods share a limit.

### Concurrency domains in `doer.json`

`doer.json` must list **every** concurrency domain the generated service sets up: named ones (`@ConcurrencyGroup`) and implicit ones (derived from a class or method name, including classes with no `@ConcurrencyLimit` and the default limit 2).

Today the `domains` array is built from the `@DoerConcurrency` annotations only. A class without the annotation still gets its own domain with limit 2 at runtime, but that domain is missing from `doer.json`. Methods also show only their own method-level `concurrency`, so a reader cannot tell which domain a method runs in.

In v1:

```json
"domains": [
    {"name": "com.example.OrderProcessor", "limit": 5, "implicit": false},
    {"name": "com.example.ShippingService", "limit": 2, "implicit": true}
],
"doer_methods": [
    {"domain": "com.example.OrderProcessor", "class": "com.example.PaymentService", "method": "payOrder", ...}
]
```

- `domains` has one entry per domain the runtime creates, sorted by name. It matches the `setupConcurrencyDomain` calls in the generated service one to one.
- `limit` is the limit the runtime uses: the declared value, or 2 when no annotation declares one. It was called `concurrency`; the new name matches `@ConcurrencyLimit`.
- `implicit` is `true` when the name was derived from a class or method rather than given in `@ConcurrencyGroup`. A domain is not implicit if any `@ConcurrencyGroup` names it. This applies even when the name equals a class name.
- Each method has a `domain` field with the resolved domain name. It replaces the per-method `concurrency` field, whose value is now on the domain.

### `ConcurrencyDomain` interface

No public method returns a `ConcurrencyDomain`, so it is public API that nobody can reach. Recommended: make it package-private for v1 and add a public accessor later if monitoring needs it. A type that is not public cannot break anyone.

If it stays public, add `DoerService.getConcurrencyDomains()` and rename `getValue()` to `getLimit()`, so the runtime view matches `@ConcurrencyLimit`.

## Updater interfaces

```java
@FunctionalInterface
public interface TaskUpdater {
    void update(Task task) throws Exception;
}

@FunctionalInterface
public interface TaskAndDataUpdater<T> {
    void update(Task task, T data) throws Exception;
}
```

The name says that both are updated: the task (its status) and the data loaded with `@TaskDataLoader`. `TaskDataUpdater` would read as if only the task data were updated.

The `facilitateCoordinatedUpdate` overloads take a different number of arguments, so lambdas and method references stay unambiguous. The same rename suggests renaming the `klazz` parameter to `dataType`.

## Exceptions

```java
public class TaskNotFoundException extends RuntimeException { long getTaskId(); }
public class TaskInProgressException extends RuntimeException { long getTaskId(); }
public class TaskVersionConflictException extends RuntimeException { long getTaskId(); }
```

There is no common base exception. `DoerService` methods also throw `SQLException`, `InterruptedException` and exceptions from user code (loaders, updaters), so a `DoerException` base could not be used to catch "everything Doer throws". Each exception is handled on its own.

`TaskVersionConflictException` is thrown when a task update finds a different `version` in the database, for example when a hijacked doer method tries to write its result.

## Migration

- All renames are source-incompatible, which v1 allows. Annotations have `CLASS` retention and the generated service is regenerated on recompilation, so for them there is nothing to keep at runtime. The interfaces are runtime types: a library compiled against 0.x that calls `facilitateCoordinatedUpdate` with `DoerTaskConsumer` or `DoerUpdater` fails at runtime with v1 until it is recompiled.
- If v1 stops retrying unannotated doer methods forever (see "Methods without `@RetryPolicy`"), every doer method that relies on endless retries needs `@RetryPolicy(interval = "5m")`. The renames break compilation, but this change does not, so the processor should warn about every doer method without `@RetryPolicy` (in the last 0.x release about methods without `@OnException`, in v1 about methods without `@RetryPolicy`). The release notes must call it out as well.
- Processor error messages that name annotations must use the new names (they currently also say `DoerParameterLoader`, a name that does not exist).
- Optional: in the last 0.x release, have the processor accept both old and new annotations and warn on the old ones. That gives users one release to migrate with compiler guidance.
- The `doer.json` format changes, and tools that read it need updating with the release:
  - `domains` lists every domain; `concurrency` becomes `limit`, and `implicit` is new;
  - entries in `doer_methods` get `domain` instead of `concurrency`;
  - `retry` and `error_status` of doer methods become `interval`, `duration` and `fallback_status`;
  - the sections `unloaders` and `extra_json_appenders` become `savers` and `exception_describers`.
- README, the tutorial repository and `sync-with-doerservice-design.md` use the old names and need updating with the release.
