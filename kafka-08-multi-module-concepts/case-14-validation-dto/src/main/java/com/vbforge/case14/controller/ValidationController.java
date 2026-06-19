package com.vbforge.case14.controller;

import com.vbforge.case14.model.OrderEventDto;
import com.vbforge.case14.model.ProducerResponse;
import com.vbforge.case14.service.ProducerService;
import com.vbforge.case14.service.ValidationConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class ValidationController {

    private final ProducerService producerService;
    private final ValidationConsumerService consumerService;

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Validation DTO Case is Running!");
    }

    // ── Stats ──

    @GetMapping("/consumer/stats")
    public ResponseEntity<ValidationConsumerService.ConsumerStats> stats() {
        return ResponseEntity.ok(consumerService.getStats());
    }

    // ── Send a VALID order ──

    @PostMapping("/producer/send/valid")
    public ResponseEntity<ProducerResponse> sendValid() {
        OrderEventDto dto = buildValidOrder();
        return ResponseEntity.ok(producerService.send(dto));
    }

    // ── Send a batch of valid orders ──

    @PostMapping("/producer/send/batch")
    public ResponseEntity<List<ProducerResponse>> sendBatch(
            @RequestParam(defaultValue = "5") int count) {
        List<ProducerResponse> responses = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            responses.add(producerService.send(buildValidOrder()));
        }
        return ResponseEntity.ok(responses);
    }

    // ── Send an INVALID order (missing required fields) ──

    // JUNIOR NOTE: These "broken" endpoints are the core of the learning experience.
    // Each one demonstrates a different category of validation failure.
    // Watch the consumer logs — violations print as WARN lines, and the rejected
    // topic receives a structured ValidationResult JSON.

    @PostMapping("/producer/send/invalid/missing-fields")
    public ResponseEntity<ProducerResponse> sendMissingFields() {
        // Missing customerId, deliveryAddress, items
        OrderEventDto dto = OrderEventDto.builder()
                .orderId("ORD-MISSING-" + UUID.randomUUID().toString().substring(0, 8))
                .orderType("STANDARD")
                .totalAmount(new BigDecimal("50.00"))
                .build();
        return ResponseEntity.ok(producerService.send(dto));
    }

    @PostMapping("/producer/send/invalid/bad-order-type")
    public ResponseEntity<ProducerResponse> sendBadOrderType() {
        // orderType "INSTANT" is not in the allowed set → @ValidOrderType fails
        OrderEventDto dto = buildValidOrder();
        dto.setOrderId("ORD-BADTYPE-" + UUID.randomUUID().toString().substring(0, 8));
        dto.setOrderType("INSTANT");
        return ResponseEntity.ok(producerService.send(dto));
    }

    @PostMapping("/producer/send/invalid/bad-amount")
    public ResponseEntity<ProducerResponse> sendBadAmount() {
        // Negative totalAmount → @DecimalMin fails
        OrderEventDto dto = buildValidOrder();
        dto.setOrderId("ORD-BADAMT-" + UUID.randomUUID().toString().substring(0, 8));
        dto.setTotalAmount(new BigDecimal("-5.00"));
        return ResponseEntity.ok(producerService.send(dto));
    }

    @PostMapping("/producer/send/invalid/bad-postal-code")
    public ResponseEntity<ProducerResponse> sendBadPostalCode() {
        // Postal code "ABCDE" doesn't match ^[0-9]{2,10}$ → @Pattern fails
        OrderEventDto dto = buildValidOrder();
        dto.setOrderId("ORD-BADZIP-" + UUID.randomUUID().toString().substring(0, 8));
        dto.getDeliveryAddress().setPostalCode("ABCDE");
        return ResponseEntity.ok(producerService.send(dto));
    }

    @PostMapping("/producer/send/invalid/empty-items")
    public ResponseEntity<ProducerResponse> sendEmptyItems() {
        // items list is empty → @Size(min=1) fails
        OrderEventDto dto = buildValidOrder();
        dto.setOrderId("ORD-EMPTYITEMS-" + UUID.randomUUID().toString().substring(0, 8));
        dto.setItems(List.of());
        return ResponseEntity.ok(producerService.send(dto));
    }

    @PostMapping("/producer/send/invalid/multiple-violations")
    public ResponseEntity<ProducerResponse> sendMultipleViolations() {
        // Several things wrong at once — demonstrates that ALL violations are reported,
        // not just the first one (Bean Validation collects them all by default)
        OrderEventDto dto = OrderEventDto.builder()
                .orderId("ORD-MULTI-" + UUID.randomUUID().toString().substring(0, 8))
                .orderType("INVALID_TYPE")
                .totalAmount(new BigDecimal("0.00"))
                // no customerId, no deliveryAddress, no items
                .build();
        return ResponseEntity.ok(producerService.send(dto));
    }


    // ===================================================================
    // HELPER
    // ===================================================================

    private OrderEventDto buildValidOrder() {
        return OrderEventDto.builder()
                .orderId("ORD-" + UUID.randomUUID().toString().substring(0, 8))
                .customerId("CUST-" + UUID.randomUUID().toString().substring(0, 6))
                .orderType("STANDARD")
                .totalAmount(new BigDecimal("149.99"))
                .deliveryAddress(OrderEventDto.DeliveryAddress.builder()
                        .street("Khreshchatyk 1")
                        .city("Kyiv")
                        .postalCode("01001")
                        .country("UA")
                        .build())
                .items(List.of(
                        OrderEventDto.OrderItem.builder()
                                .itemId("ITEM-001")
                                .itemName("Spring Boot in Action")
                                .quantity(1)
                                .unitPrice(new BigDecimal("49.99"))
                                .build(),
                        OrderEventDto.OrderItem.builder()
                                .itemId("ITEM-002")
                                .itemName("Kafka: The Definitive Guide")
                                .quantity(2)
                                .unitPrice(new BigDecimal("50.00"))
                                .build()
                ))
                .createdAt(LocalDateTime.now())
                .build();
    }

}
