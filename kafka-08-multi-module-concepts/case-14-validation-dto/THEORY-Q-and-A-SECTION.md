# Case 14: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Bean Validation (JSR-380)** | Specification (now Jakarta Validation) for declarative constraint annotation on Java objects |
| **Hibernate Validator** | Reference implementation of Bean Validation — the actual enforcement engine |
| **`@Valid`** | Triggers validation on the annotated parameter or field; enables recursion into nested objects |
| **`ConstraintValidator<A, T>`** | Interface for custom validators — `A` is the annotation, `T` is the validated type |
| **`Set<ConstraintViolation<T>>`** | Result of `validator.validate()` — one entry per failed constraint |
| **`getPropertyPath()`** | Reports the field path (including nested: `deliveryAddress.postalCode`) |
| **Rejection pattern** | Commit the offset for invalid messages after routing them to a rejected topic |

### The Constraint Annotation Structure

```
@Documented
@Constraint(validatedBy = MyValidator.class)   ← links annotation to implementation
@Target({ElementType.FIELD, ...})
@Retention(RetentionPolicy.RUNTIME)             ← REQUIRED — must survive to runtime
public @interface MyConstraint {
    String message() default "...";             ← REQUIRED by spec
    Class<?>[] groups() default {};             ← REQUIRED by spec
    Class<? extends Payload>[] payload() default {};  ← REQUIRED by spec
}
```

---

## 📝 Interview Q&A

### Q1: What is the difference between JSR-380, Jakarta Validation, and Hibernate Validator?

**Answer:**

These three things are often conflated but play different roles:

**JSR-380** (Bean Validation 2.0) is the Java specification request — a document that defines the API, the annotations (`@NotNull`, `@Size`, etc.), and the contracts. The `jakarta.validation` package contains only interfaces and annotations, no implementation.

**Jakarta Validation** (formerly `javax.validation`) is the package that houses the spec API after the transition from the `javax.*` namespace to `jakarta.*` (part of the Jakarta EE 9+ move). If you've seen code that uses `javax.validation.constraints.NotNull`, that's the pre-Jakarta version; the same annotation is now at `jakarta.validation.constraints.NotNull`. Spring Boot 3+ and 4+ use the Jakarta namespace.

**Hibernate Validator** is the reference implementation — the actual library (`hibernate-validator-*.jar`) that enforces the constraints at runtime. When you call `Validator.validate()`, it's Hibernate Validator running the logic. `spring-boot-starter-validation` pulls it in automatically.

Practical consequence: you write annotations from the `jakarta.validation.*` spec package, but the engine running them is Hibernate Validator. You can swap Hibernate Validator for another implementation (Apache BVal), but in practice everyone uses Hibernate Validator.

---

### Q2: What is the difference between `@NotNull`, `@NotEmpty`, and `@NotBlank`?

**Answer:**

These three annotations handle progressively stricter not-emptiness checks:

`@NotNull` — the value must not be `null`. A string `""` (empty) or `"   "` (whitespace-only) passes.

`@NotEmpty` — applies to strings, collections, maps, and arrays. For strings: not null AND `length > 0`. A whitespace-only string `"   "` still passes. For collections: not null AND `size > 0`.

`@NotBlank` — applies to strings only. Not null AND length after trimming whitespace must be > 0. This is the strictest option for strings: null, `""`, and `"   "` all fail.

Rule of thumb for string fields:
- IDs, names, codes: use `@NotBlank`
- Collections/arrays: use `@NotEmpty` (or `@Size(min=1)`)
- Object references where only null matters: use `@NotNull`

Trying to use `@NotBlank` on a `List` will throw a `ConstraintDeclarationException` at startup — Hibernate Validator enforces type compatibility between annotation and target type.

---

### Q3: How does `@Valid` enable recursive validation of nested objects?

**Answer:**

Without `@Valid`, Hibernate Validator treats a nested object field as an opaque reference. It applies null checks (if you put `@NotNull` on the field), but does NOT recurse into the object's fields. The constraints inside the nested class are invisible to the outer validator call.

With `@Valid` on the field, Hibernate Validator calls `validate()` recursively on the nested object and adds its violations to the parent's result set. The `getPropertyPath()` on each violation from the nested object includes the full path: `deliveryAddress.postalCode`, not just `postalCode`.

This cascade is optional because sometimes you want to validate the reference (is it null?) independently from the contents. Example: an optional shipping address — `@Valid` only if the address is non-null. Spring supports this pattern with `@Valid` combined with `@NotNull`: `@NotNull` fires on null, `@Valid` fires recursively when non-null.

The same principle applies to collection elements: `@Valid List<OrderItem> items` recurses into each `OrderItem` and prefixes violations with the index path: `items[0].unitPrice`, `items[2].itemName`.

---

### Q4: How do you create a custom constraint validator? What are the three mandatory annotation attributes?

**Answer:**

Creating a custom constraint requires two files:

**The annotation** — a `@interface` annotated with `@Constraint(validatedBy = YourValidator.class)`, `@Retention(RetentionPolicy.RUNTIME)`, and one or more `@Target` values. It must declare three attributes that the Bean Validation spec requires:

```java
String message() default "...";
Class<?>[] groups() default {};
Class<? extends Payload>[] payload() default {};
```

Omitting any of these causes `ConstraintDefinitionException` at startup. `groups` enables conditional validation (validation groups — an advanced topic). `payload` is a pass-through mechanism for metadata consumers.

**The validator** — implements `ConstraintValidator<A, T>` where `A` is your annotation and `T` is the type it validates. Two methods:
- `initialize(A annotation)` — reads annotation attributes once at instantiation. If your annotation has configurable parameters, read them here.
- `isValid(T value, ConstraintValidatorContext context)` — returns true/false. **Always return true for null** — let `@NotNull`/`@NotBlank` handle null checks separately so your custom validator is composable.

One validator class can implement `ConstraintValidator` multiple times for different types via Java's generic type system (one implementation per type target).

---

### Q5: `validator.validate(dto)` vs `@Valid` on a method parameter — when would you use each?

**Answer:**

`@Valid` on a method parameter (most common in `@RestController` methods) tells Spring to run Bean Validation before your method executes. If violations exist, Spring throws `MethodArgumentNotValidException` and your method is never called. The error response is generated by the global `@ControllerAdvice` exception handler (or the default Spring error handling). You have limited context — you know validation failed, but you're in an error handler, not your business logic.

`Validator.validate(dto)` gives you programmatic control inside your own code. You decide what to do with the violations: log them, send to a rejected topic, return a custom response, retry with different data, etc. You can access the full violation set, field paths, and messages in normal application code.

In the Kafka consumer case (`case-14`), `@Valid` on a `@KafkaListener` parameter would route failures to the error handler, which would have no way to publish a structured `ValidationResult` to the rejected topic. The manual approach is the right tool here.

General rule: use `@Valid` in controllers for HTTP request DTOs where Spring's error handling is sufficient. Use `Validator.validate()` in services, consumers, or batch processors where you need to act on violations programmatically.

---

### Q6: Why do we `ack.acknowledge()` even for rejected (invalid) messages?

**Answer:**

Because invalid-message rejection is a **business decision**, not a Kafka error.

A Kafka error would be: "I tried to process this message and something went wrong technically — I should retry." In that case, NOT committing makes sense: let Kafka redeliver the message, try again.

But an invalid message is different: "I received this message, examined it, determined it violates business rules, recorded that decision (to the rejected topic), and I am done with it." From Kafka's perspective the message was handled — it got a response. Committing the offset is the correct outcome.

NOT committing an invalid message would cause infinite redelivery: the consumer restarts, sees the same invalid message, rejects it again, doesn't commit, restarts... This is a liveness failure — the partition is stuck and no subsequent messages on that partition are ever processed.

The distinction maps to HTTP: 400 Bad Request still sends a response — the request was received and processed (rejected), not lost. Not sending any response would leave the client hanging forever.

---

### Q7: What does `getPropertyPath()` return for a nested constraint violation?

**Answer:**

`ConstraintViolation.getPropertyPath()` returns a `Path` object representing the traversal path from the root validated object to the field that failed. When converted to string (`.toString()`), it gives a dot-separated path.

Examples:
- Top-level field `orderId` fails: `"orderId"`
- Nested object field fails: `"deliveryAddress.postalCode"`
- Collection element field fails: `"items[0].unitPrice"` (index in brackets)
- Double-nested: `"order.deliveryAddress.country"`

This path is extremely useful for error response construction — you can map each violation to its field path, giving the API consumer precise feedback about which field failed and why, without exposing internal Java class structure.

In a REST API error response pattern:
```json
{
  "errors": [
    { "field": "deliveryAddress.postalCode", "message": "postalCode must be 2–10 digits" },
    { "field": "items[1].quantity", "message": "quantity must be at least 1" }
  ]
}
```

---

### Q8: What is the relationship between Bean Validation groups and validation ordering?

**Answer:**

Validation **groups** let you apply different subsets of constraints depending on context. The canonical example: a form that shows different fields on create vs. update.

```java
interface OnCreate {}
interface OnUpdate {}

@NotBlank(groups = OnCreate.class)
private String password;

@NotBlank(groups = {OnCreate.class, OnUpdate.class})
private String email;
```

`validator.validate(dto)` validates only the **default group** (constraints with no explicit group, or `Default.class`). To validate a specific group: `validator.validate(dto, OnCreate.class)`. Spring MVC supports groups via `@Validated(OnCreate.class)` (not `@Valid`, which only runs the default group).

**Ordering** is a separate feature via `@GroupSequence`. Without it, all constraint violations are discovered in one pass — no ordering guarantee between groups, all violations returned together. With `@GroupSequence({FirstGroup.class, SecondGroup.class})`, Hibernate Validator runs the first group, and only proceeds to the second group if the first group passes completely. This is the "fail fast at stage 1" pattern: first validate the structure is parseable, then validate business rules only if structure is valid.

In case-14, we use only the default group (no explicit groups on any annotation), which is the correct starting point. Groups are a power feature for complex multi-step validation scenarios.

---

## 📊 Quick Reference Card

| Annotation | What it checks | Applies to |
|------------|----------------|------------|
| `@NotNull` | Not null | Any |
| `@NotEmpty` | Not null, not empty | String, Collection, Map, Array |
| `@NotBlank` | Not null, not whitespace-only | String |
| `@Size(min, max)` | Length or size in range | String, Collection, Map, Array |
| `@Min(n)` / `@Max(n)` | Numeric value ≥ n / ≤ n | int, long, short, byte (and wrappers) |
| `@DecimalMin` / `@DecimalMax` | Numeric value in range (string-specified) | BigDecimal, BigInteger, String, and numeric types |
| `@Pattern(regexp)` | String matches regex | String |
| `@Email` | Valid email format | String |
| `@Valid` | Recurse into nested object | Object field, Collection element |
| `@Validated` | Spring's variant of @Valid — supports groups | Controller method parameter |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Distinguish JSR-380 (spec), Jakarta Validation (package), and Hibernate Validator (implementation)
- Choose the right annotation among `@NotNull`, `@NotEmpty`, `@NotBlank` for a given field
- Explain why `@Valid` is required for recursive nested object validation
- Create a custom `ConstraintValidator` with the correct annotation structure and null handling
- Choose between `@Valid`/`@Validated` and programmatic `Validator.validate()` for a given context
- Explain why rejected (invalid) Kafka messages should still be acknowledged
- Read the full property path from a nested constraint violation
- Explain what validation groups are and when you'd use them
