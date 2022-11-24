package org.acme.services;

import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.acme.model.AmqpMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.buffer.Buffer;
import io.vertx.rabbitmq.RabbitMQClient;

@ApplicationScoped
public class GreetingService {
    private static final Logger log = LoggerFactory.getLogger(GreetingService.class);

    public static final String MP_GREETING_EXCHANGE = "greeting";
    public static final String EXCHANGE = "manual-greeting";
    public static final String GREETING_QUEUE = "manual-greeting";
    public static final String ROUTING_KEY = "greeting";
    public static final String MESSAGE_HEADER_CORRELATION_ID = "correlationId";
    public static final String MESSAGE_HEADER_EVENT_TYPE = "eventType";
    public static final String MESSAGE_HEADER_SESSION_ID = "sessionId";

    @Inject
    ObjectMapper objectMapper;

    @Inject
    RabbitMQClient client;

    void createChanel(@Observes StartupEvent event) {
        client.start(asyncResult -> {
            if (asyncResult.succeeded()) {
                log.info("RabbitMQ successfully connected!");
                client.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT.getType(), false, false)
                        .onSuccess(unused -> {
                            final var correlationId = UUID.randomUUID().toString();
                            MDC.put(MESSAGE_HEADER_CORRELATION_ID, correlationId);
                            sendToExchange(correlationId, "Hello from Greeting service");
                        });
            } else {
                log.info("Fail to connect to RabbitMQ {}", asyncResult.cause().getMessage());
            }
        });
    }

    public void sendToExchange(String correlationId, String message) {
        log.info("Sending message to exchanges: [{}, {}], message: {}", EXCHANGE, MP_GREETING_EXCHANGE, message);
        final var amqpMessage = createAmqpMessage(correlationId, "greeting", message);
        client.basicPublish(EXCHANGE, ROUTING_KEY, amqpMessage.properties(), amqpMessage.body());
        client.basicPublish(MP_GREETING_EXCHANGE, ROUTING_KEY, amqpMessage.properties(), amqpMessage.body());
    }

    public AmqpMessage createAmqpMessage(String correlationId, String eventType, String message) {
        var headers = new HashMap<String, Object>();
        headers.put(MESSAGE_HEADER_CORRELATION_ID, correlationId);
        headers.put(MESSAGE_HEADER_EVENT_TYPE, eventType);
        log.info("backend payload: {},  headers: {}", message, headers);
        final AMQP.BasicProperties messageProperties = new AMQP.BasicProperties()
                .builder()
                .correlationId(correlationId)
                .timestamp(new Date())
                .headers(headers)
                .build();

        return new AmqpMessage(writeValueAsBytes(message), messageProperties, eventType);
    }

    private Buffer writeValueAsBytes(Object value) throws RuntimeException {
        try {
            return Buffer.buffer(objectMapper.writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not serialize to json", e);
        }
    }
}
