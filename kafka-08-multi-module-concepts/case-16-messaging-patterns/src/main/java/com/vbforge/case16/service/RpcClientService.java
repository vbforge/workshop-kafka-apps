package com.vbforge.case16.service;

import com.vbforge.case16.model.RequestMessage;
import com.vbforge.case16.model.ReplyMessage;
import com.vbforge.case16.model.RpcResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// JUNIOR NOTE: This is the CLIENT side of the Kafka RPC pattern.
//
// The flow:
//   1. Build a ProducerRecord for the request topic
//   2. Add KafkaHeaders.REPLY_TOPIC header — tells the server WHERE to reply
//   3. Call replyingKafkaTemplate.sendAndReceive() — this:
//        a) adds a CORRELATION_ID header automatically
//        b) registers a pending future keyed on that correlation ID
//        c) sends the record to the request topic
//        d) blocks (async) waiting for a matching reply
//   4. When the reply arrives on the reply topic, the template matches the
//      CORRELATION_ID header, resolves the future, and your code unblocks
//
// This is synchronous from the HTTP caller's perspective — the REST endpoint
// blocks until the reply arrives or times out. In production you'd return
// a 202 Accepted with a tracking ID and poll separately — but for learning
// purposes, blocking is cleaner to follow.

@Service
@Slf4j
public class RpcClientService {

    private final ReplyingKafkaTemplate<String, Object, ReplyMessage> replyingKafkaTemplate;

    @Value("${kafka.topics.request}")
    private String requestTopic;

    @Value("${kafka.topics.reply}")
    private String replyTopic;

    @Value("${kafka.consumer.request-timeout-ms}")
    private long requestTimeoutMs;

    public RpcClientService(ReplyingKafkaTemplate<String, Object, ReplyMessage> replyingKafkaTemplate) {
        this.replyingKafkaTemplate = replyingKafkaTemplate;
    }

    public RpcResponse sendAndReceive(String payload, String priority) {
        String requestId = UUID.randomUUID().toString();
        long startMs = System.currentTimeMillis();

        RequestMessage request = RequestMessage.builder()
                .requestId(requestId)
                .payload(payload)
                .priority(priority)
                .sentAt(LocalDateTime.now())
                .build();

        // JUNIOR NOTE: ProducerRecord is built manually so we can add the
        // REPLY_TOPIC header. Without this header, the server-side @KafkaListener
        // with @SendTo won't know where to send the reply.
        ProducerRecord<String, Object> record = new ProducerRecord<>(requestTopic, requestId, request);
        record.headers().add(new RecordHeader(
                KafkaHeaders.REPLY_TOPIC,
                replyTopic.getBytes(StandardCharsets.UTF_8)
        ));

        // JUNIOR NOTE: Also adding a custom "priority" header so the header-routing
        // demo can read it directly from Kafka headers without deserializing the body.
        record.headers().add(new RecordHeader(
                "priority",
                priority.getBytes(StandardCharsets.UTF_8)
        ));

        log.info(">>> [RPC-CLIENT] Sending request id={} payload='{}' priority={}",
                requestId, payload, priority);

        try {
            RequestReplyFuture<String, Object, ReplyMessage> future =
                    replyingKafkaTemplate.sendAndReceive(record);

            // JUNIOR NOTE: future.getSendFuture().get() waits until the REQUEST was
            // successfully published to Kafka (ack from broker). Separate from the reply.
            future.getSendFuture().get(5, TimeUnit.SECONDS);
            log.debug(">>> [RPC-CLIENT] Request published to broker");

            // JUNIOR NOTE: future.get() waits for the REPLY to arrive.
            // This is where the blocking happens. The timeout must be generous enough
            // for the server to process and reply, but short enough to fail fast on errors.
            ConsumerRecord<String, ReplyMessage> replyRecord =
                    future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);

            ReplyMessage reply = replyRecord.value();
            long roundTripMs = System.currentTimeMillis() - startMs;

            log.info(">>> [RPC-CLIENT] Reply received for id={} success={} roundTripMs={}",
                    requestId, reply.isSuccess(), roundTripMs);

            return RpcResponse.builder()
                    .requestId(requestId)
                    .result(reply.getResult())
                    .success(reply.isSuccess())
                    .roundTripMs(roundTripMs)
                    .completedAt(LocalDateTime.now())
                    .build();

        } catch (TimeoutException e) {
            log.error(">>> [RPC-CLIENT] Timeout waiting for reply id={}", requestId);
            return RpcResponse.builder()
                    .requestId(requestId)
                    .result(null)
                    .success(false)
                    .roundTripMs(System.currentTimeMillis() - startMs)
                    .completedAt(LocalDateTime.now())
                    .build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("RPC interrupted for request " + requestId, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("RPC failed for request " + requestId + ": " + e.getCause().getMessage(), e);
        }
    }

}
