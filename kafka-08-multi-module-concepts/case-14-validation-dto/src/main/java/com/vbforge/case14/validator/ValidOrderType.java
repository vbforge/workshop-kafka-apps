package com.vbforge.case14.validator;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.*;

// JUNIOR NOTE: Creating a custom Bean Validation constraint requires two pieces:
//
//   1. The ANNOTATION (this file) — declares the constraint, links to the validator,
//      and provides the default message, groups, and payload.
//
//   2. The VALIDATOR class (OrderTypeValidator.java) — implements ConstraintValidator<A, T>
//      where A is this annotation and T is the type it validates (String here).
//
// The @Constraint(validatedBy = ...) link is what connects them.
// When Hibernate Validator sees @ValidOrderType on a field, it instantiates
// OrderTypeValidator and calls its isValid() method.
//
// @Documented → the annotation appears in generated Javadoc for the annotated field.
// @Retention(RUNTIME) → required! Without RUNTIME retention, the JVM discards
//   the annotation before Hibernate Validator can read it via reflection.
// @Target → which program elements can carry this annotation. FIELD + PARAMETER
//   covers the common cases; add METHOD if you want to use it on method return values.

@Documented
@Constraint(validatedBy = OrderTypeValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidOrderType {

    // JUNIOR NOTE: message(), groups(), and payload() are MANDATORY by the Bean Validation spec.
    // If you omit any of them, the container throws a ConstraintDefinitionException at startup.
    String message() default "orderType must be one of: STANDARD, EXPRESS, BULK, DIGITAL";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
