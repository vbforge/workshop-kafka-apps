package com.vbforge.case14.model;

import com.vbforge.case14.validator.ValidOrderType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

// JUNIOR NOTE: This DTO is the main learning artifact of case-14.
// Bean Validation (JSR-380 / Jakarta Validation) works by annotating fields
// with constraint annotations. Hibernate Validator (the reference implementation,
// pulled in by spring-boot-starter-validation) processes these annotations
// at validation time — either triggered by @Valid in a controller, or manually
// via javax.validation.Validator.
//
// Two categories of constraints shown here:
//   1. Built-in constraints from jakarta.validation.constraints.*
//      @NotNull, @NotBlank, @Size, @Min, @Max, @DecimalMin, @Pattern, @Email etc.
//   2. Custom constraint: @ValidOrderType — see ValidOrderType.java + OrderTypeValidator.java
//
// In this case we validate MANUALLY inside the consumer service (not via @Valid),
// so you can see the full validation API: Validator.validate() → Set<ConstraintViolation>
// → extract messages → decide what to do (reject, send to rejected topic, etc.)

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderEventDto {

    // JUNIOR NOTE: @NotBlank is stricter than @NotNull — it rejects null, empty (""), AND whitespace-only ("   ").
    // For string IDs and names, @NotBlank is almost always what you want.
    @NotBlank(message = "orderId must not be blank")
    @Size(max = 64, message = "orderId must not exceed 64 characters")
    private String orderId;

    @NotBlank(message = "customerId must not be blank")
    private String customerId;

    // JUNIOR NOTE: Custom annotation — @ValidOrderType checks against a fixed enum set.
    // This shows how to write a validator when no built-in constraint fits your business rule.
    @NotBlank(message = "orderType must not be blank")
    @ValidOrderType
    private String orderType;

    // JUNIOR NOTE: @DecimalMin with inclusive=false means strictly greater than 0.00.
    // Using BigDecimal for monetary values — never use double/float for money (precision loss).
    @NotNull(message = "totalAmount must not be null")
    @DecimalMin(value = "0.01", message = "totalAmount must be greater than 0")
    private BigDecimal totalAmount;

    // JUNIOR NOTE: @Valid on a nested object triggers recursive validation.
    // Without @Valid, Hibernate Validator skips DeliveryAddress entirely —
    // it validates the reference (is it non-null?) but NOT the fields inside it.
    @NotNull(message = "deliveryAddress must not be null")
    @Valid
    private DeliveryAddress deliveryAddress;

    // JUNIOR NOTE: @Size on a List validates the collection size, not the element values.
    // To also validate element values, annotate each element's class with constraints
    // (and use @Valid here to recurse into them).
    @NotNull(message = "items must not be null")
    @Size(min = 1, message = "order must contain at least one item")
    @Valid
    private List<OrderItem> items;

    private LocalDateTime createdAt;


    // ====================================================================
    // NESTED CLASSES — each with their own Bean Validation constraints
    // ====================================================================

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class DeliveryAddress {

        @NotBlank(message = "street must not be blank")
        private String street;

        @NotBlank(message = "city must not be blank")
        private String city;

        // JUNIOR NOTE: @Pattern validates a string against a regex.
        // Here we enforce a simple numeric postal code (2–10 digits).
        // In a real system you'd have country-specific patterns.
        @NotBlank(message = "postalCode must not be blank")
        @Pattern(regexp = "^[0-9]{2,10}$", message = "postalCode must be 2–10 digits")
        private String postalCode;

        @NotBlank(message = "country must not be blank")
        @Size(min = 2, max = 2, message = "country must be a 2-letter ISO code")
        private String country;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class OrderItem {

        @NotBlank(message = "itemId must not be blank")
        private String itemId;

        @NotBlank(message = "itemName must not be blank")
        @Size(max = 100, message = "itemName must not exceed 100 characters")
        private String itemName;

        // JUNIOR NOTE: @Min on an int/long checks the numeric value.
        // Note: @Min/@Max work on int, long, short, byte and their wrappers.
        // For BigDecimal use @DecimalMin/@DecimalMax.
        @Min(value = 1, message = "quantity must be at least 1")
        @Max(value = 1000, message = "quantity must not exceed 1000")
        private int quantity;

        @NotNull(message = "unitPrice must not be null")
        @DecimalMin(value = "0.01", message = "unitPrice must be greater than 0")
        private BigDecimal unitPrice;
    }

}
