package org.acme.services;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.acme.model.AmqpMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import io.quarkus.runtime.StartupEvent;

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
    RabbitMQClient rabbitMQClient;

    private Channel channel;

    public void onApplicationStart(@Observes StartupEvent event) {
        setupQueues();
    }

    protected void setupQueues() {
        try {
            // create a connection
            Connection connection = rabbitMQClient.connect();
            // create a channel
            channel = connection.createChannel();
            // declare exchange
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void sendToExchange(String correlationId, String message) {
        log.info("Sending message to exchanges: [{}, {}], message: {}", EXCHANGE, MP_GREETING_EXCHANGE, message);
        final var amqpMessage = createAmqpMessage(correlationId, "greeting", message);
        try {
            channel.basicPublish(EXCHANGE, ROUTING_KEY, amqpMessage.properties(), amqpMessage.body());
            channel.basicPublish(MP_GREETING_EXCHANGE, ROUTING_KEY, amqpMessage.properties(), amqpMessage.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

        return new AmqpMessage(message.getBytes(StandardCharsets.UTF_8), messageProperties, eventType);
    }
}
