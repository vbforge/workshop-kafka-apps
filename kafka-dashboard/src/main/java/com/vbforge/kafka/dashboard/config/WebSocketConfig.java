package com.vbforge.kafka.dashboard.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
 
/**
 * WebSocket / STOMP broker configuration.
 *
 * WHY SockJS WAS REMOVED:
 *
 * SockJS performs an HTTP GET to /ws-dashboard/info before upgrading to WebSocket.
 * On Spring Boot 3.x this probe request is processed by the DispatcherServlet
 * filter chain, which can reject it with 403 or fail the CORS preflight even
 * when no Spring Security dependency is present. The result: SockJS silently
 * falls back to long-polling, STOMP never connects, WS stays "CONNECTING..." forever.
 *
 * Native WebSocket (no SockJS) skips that probe entirely — the browser upgrades
 * the connection directly via the HTTP Upgrade header. Modern browsers (Chrome,
 * Firefox, Edge, Safari) all support native WebSocket. SockJS is only needed
 * for IE9 and very old environments, which is not our use case.
 *
 * The JS client side must also change accordingly — see dashboard.html.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {
 
    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        config.enableSimpleBroker("/topic");
        config.setApplicationDestinationPrefixes("/app");
    }
 
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws-dashboard")
                .setAllowedOriginPatterns("*");
        // NO .withSockJS() — native WebSocket only.
        // This eliminates the /info probe that was blocking the connection.
    }
}