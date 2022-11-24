package org.acme.websocket;

import static org.acme.services.GreetingService.MESSAGE_HEADER_CORRELATION_ID;
import static org.acme.services.GreetingService.MESSAGE_HEADER_SESSION_ID;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.acme.services.GreetingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import io.undertow.websockets.DefaultContainerConfigurator;

@ServerEndpoint(value = "/v0/websocket", configurator = DefaultContainerConfigurator.class)
@ApplicationScoped
public class WebSocket {
    private static final Logger log = LoggerFactory.getLogger(WebSocket.class);

    @Inject
    GreetingService greetingService;

    @OnOpen
    public void onOpen(Session session, EndpointConfig conf) {
        try {
            MDC.put(MESSAGE_HEADER_SESSION_ID, session.getId());
            log.info("Web socket opened. userProperties: {} ", conf.getUserProperties());
            Map<String, List<String>> headers = Optional
                    .ofNullable((Map<String, List<String>>) conf.getUserProperties().remove("headers"))
                    .orElse(Collections.emptyMap());
        } finally {
            MDC.clear();
        }
    }

    @OnClose
    public void onClose(Session session, CloseReason reason) {
        try {
            MDC.put(MESSAGE_HEADER_SESSION_ID, session.getId());
            log.info("Closing websocket user session. reason: {}, closeCode: {}", reason.getReasonPhrase(),
                    reason.getCloseCode());
        } finally {
            MDC.clear();
        }
    }

    @OnError
    public void onError(Session session, Throwable throwable) {
        log.warn("on error happened", throwable);
        onClose(session, new CloseReason(CloseReason.CloseCodes.CLOSED_ABNORMALLY, "error " + throwable.getMessage()));
    }

    @OnMessage
    public void onMessage(Session session, String message) {
        try {
            MDC.put(MESSAGE_HEADER_SESSION_ID, session.getId());
            if (message.isEmpty()) {
                log.warn("Received empty message, discarding message.");
                return;
            }

            final var correlationId = UUID.randomUUID().toString();
            MDC.put(MESSAGE_HEADER_CORRELATION_ID, correlationId);
            log.info("Handling websocket client message.");
            greetingService.sendToExchange(correlationId, message);
        } catch (Exception e) {
            log.error("Could not handle websocket client message", e);
            session.getAsyncRemote().sendText("Could not read message " + e.getMessage());
        } finally {
            MDC.clear();
        }
    }
}
