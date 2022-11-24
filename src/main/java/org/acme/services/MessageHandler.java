package org.acme.services;

import static org.acme.services.GreetingService.EXCHANGE;
import static org.acme.services.GreetingService.GREETING_QUEUE;
import static org.acme.services.GreetingService.MESSAGE_HEADER_CORRELATION_ID;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    @Inject
    RabbitMQClient rabbitMQClient;

    private Channel channel;

    public void onApplicationStart(@Observes StartupEvent event) {
        setupQueues();
        setupReceiving();
    }

    protected void setupQueues() {
        try {
            // create a connection
            Connection connection = rabbitMQClient.connect();
            // create a channel
            channel = connection.createChannel();
            // declare exchanges and queues
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT, false);
            channel.queueDeclare(GREETING_QUEUE, false, false, false, null);
            channel.queueBind(GREETING_QUEUE, EXCHANGE, "#");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void setupReceiving() {
        try {
            // register a consumer for messages
            channel.basicConsume(GREETING_QUEUE, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                        byte[] body) {
                    MDC.put(MESSAGE_HEADER_CORRELATION_ID, properties.getCorrelationId());
                    // just print the received message.
                    log.info("Received: " + new String(body, StandardCharsets.UTF_8));
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
