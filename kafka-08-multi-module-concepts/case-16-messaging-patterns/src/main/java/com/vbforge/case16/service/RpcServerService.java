package com.vbforge.case16.service;

import com.vbforge.case16.model.RequestMessage;
import com.vbforge.case16.model.ReplyMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

// JUNIOR NOTE: This is the SERVER side of the Kafka RPC pattern.
//
// The @KafkaListener receives the RequestMessage. @SendTo tells Spring Kafka
// to take the method's RETURN VALUE and publish it to the reply topic.
//
// Spring Kafka handles the routing automatically:
//   - It reads the KafkaHeaders.REPLY_TOPIC header from the incoming request
//   - It reads the KafkaHeaders.CORRELATION_ID header
//   - After the method returns, it publishes the ReplyMessage to the reply topic
//     WITH the same CORRELATION_ID header set
//   - The client's ReplyingKafkaTemplate sees the matching CORRELATION_ID
//     and completes the pending future
//
// You never write any of this plumbing yourself — @SendTo + the listener
// container factory with setReplyTemplate() does it all.
//
// containerFactory = "listenerContainerFactory" — must reference the bean that
// has setReplyTemplate() configured. Using the wrong factory = no reply sent.

@Service
@Slf4j
public class RpcServerService {

    private final AtomicLong requestsHandled = new AtomicLong(0);

    @KafkaListener(
            topics = "${kafka.topics.request}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "listenerContainerFactory"
    )
    @SendTo   // JUNIOR NOTE: @SendTo with no argument uses the REPLY_TOPIC header from the request.
              // You can also @SendTo("specific-topic") to hard-code the reply destination,
              // but using the header makes the server generic — any client can set its own reply topic.
    public ReplyMessage handleRequest(RequestMessage request) {
        long count = requestsHandled.incrementAndGet();
        log.info(">>> [RPC-SERVER] Handling request #{} id={} payload='{}'",
                count, request.getRequestId(), request.getPayload());

        try {
            // Simulate processing — in a real system: DB lookup, computation, external API call
            String result = processPayload(request.getPayload());

            log.info(">>> [RPC-SERVER] Sending reply for id={} result='{}'",
                    request.getRequestId(), result);

            return ReplyMessage.builder()
                    .requestId(request.getRequestId())
                    .result(result)
                    .success(true)
                    .processedAt(LocalDateTime.now())
                    .build();

        } catch (Exception e) {
            log.error(">>> [RPC-SERVER] Error processing request id={}: {}",
                    request.getRequestId(), e.getMessage());

            // JUNIOR NOTE: Even on error, we return a ReplyMessage (with success=false)
            // rather than throwing. Throwing from a @KafkaListener with @SendTo would
            // prevent the reply from being sent — the client would time out instead of
            // getting a useful error response. Always reply, even for failures.
            return ReplyMessage.builder()
                    .requestId(request.getRequestId())
                    .result(null)
                    .success(false)
                    .errorMessage(e.getMessage())
                    .processedAt(LocalDateTime.now())
                    .build();
        }
    }

    private String processPayload(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("Payload must not be blank");
        }
        // Simulate some processing work
        return "PROCESSED: " + payload.toUpperCase() + " [seq=" + requestsHandled.get() + "]";
    }

    public long getRequestsHandled() {
        return requestsHandled.get();
    }

}
