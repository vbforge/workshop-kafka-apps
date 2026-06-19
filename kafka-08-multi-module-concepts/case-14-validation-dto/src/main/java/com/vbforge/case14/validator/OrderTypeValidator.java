package com.vbforge.case14.validator;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.Set;

// JUNIOR NOTE: ConstraintValidator<A, T>
//   A = the annotation this validator handles → ValidOrderType
//   T = the type of the value being validated → String
//
// The lifecycle:
//   1. initialize(annotation) — called once by Hibernate Validator when the
//      validator is instantiated. Use it to read annotation attributes
//      (e.g., the allowed values list if it were configurable).
//   2. isValid(value, context) — called once per validated field.
//      Return true → passes. Return false → violation with the annotation's message.
//
// IMPORTANT: isValid must handle null gracefully. By convention, null is valid
// here (let @NotBlank / @NotNull handle the null check separately — don't conflate
// "is null" with "is not a known order type"). This is the single-responsibility
// principle applied to validators.

public class OrderTypeValidator implements ConstraintValidator<ValidOrderType, String> {

    private static final Set<String> ALLOWED_TYPES = Set.of(
            "STANDARD", "EXPRESS", "BULK", "DIGITAL"
    );

    @Override
    public void initialize(ValidOrderType annotation) {
        // Nothing to initialize here — ALLOWED_TYPES is a compile-time constant.
        // If the allowed values came from the annotation itself
        // (e.g., @ValidOrderType(allowed = {"STANDARD","EXPRESS"}))
        // you'd read annotation.allowed() here and store them for isValid().
    }

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        // JUNIOR NOTE: Explicit null check is the canonical pattern for custom validators.
        // If null reaches here (because @NotBlank wasn't applied first, or the field
        // is optional), we return true and let the null-specific annotation handle it.
        if (value == null) {
            return true;
        }
        return ALLOWED_TYPES.contains(value.toUpperCase());
    }

}
