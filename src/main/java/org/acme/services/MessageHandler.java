package org.acme.services;

import static org.acme.services.GreetingService.EXCHANGE;
import static org.acme.services.GreetingService.GREETING_QUEUE;
import static org.acme.services.GreetingService.MESSAGE_HEADER_CORRELATION_ID;
import static org.acme.services.GreetingService.MESSAGE_HEADER_EVENT_TYPE;
import static org.acme.services.GreetingService.ROUTING_KEY;

import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.rabbitmq.client.BuiltinExchangeType;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.QueueOptions;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import io.vertx.rabbitmq.RabbitMQMessage;

@ApplicationScoped
public class MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    @Inject
    RabbitMQClient client;

    public void onApplicationStart(@Observes StartupEvent event) {
        createExchangeAndBindQueues();
        client.start(asyncResult -> {
            if (asyncResult.succeeded()) {
                log.info("RabbitMQ successfully connected for queue {}!", GREETING_QUEUE);
            } else {
                log.error(String.format("Failed to connect to RabbitMQ for '%s' consumer. Cause: %s", GREETING_QUEUE,
                                asyncResult.cause().getMessage()),
                        asyncResult.cause());
            }
        });
    }

    protected void createExchangeAndBindQueues() {
        final var args = new JsonObject();
        args.put("x-message-ttl", 5000);
        client.addConnectionEstablishedCallback(
                promise -> client.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT.getType(), false, false)
                        .compose(dok -> client.queueDeclare(GREETING_QUEUE, false, false, false, args))
                        .compose(dok -> client.queueBind(GREETING_QUEUE, EXCHANGE, ROUTING_KEY))
                        .onSuccess(unused -> setListener(GREETING_QUEUE))
                        .onComplete(promise));
    }

    protected void setListener(String queueName) {
        var options = new QueueOptions();
        options.setAutoAck(true);
        options.setMaxInternalQueueSize(50);

        client.basicConsumer(queueName, options, result -> {
            if (result.succeeded()) {
                RabbitMQConsumer mqConsumer = result.result();
                mqConsumer.handler(message -> {
                    final var newDuplicatedContext = VertxContext.createNewDuplicatedContext();
                    VertxContext.runOnContext(newDuplicatedContext, () -> handleMessage(message));
                });
                log.info("consumer started on {}", queueName);
            } else {
                log.error("error", result.cause());
            }
        });
    }

    private void handleMessage(RabbitMQMessage message) {
        var properties = message.properties();
        var body = message.body();
        final var correlationId = properties.getCorrelationId();
        final var eventType = properties.getHeaders().get(MESSAGE_HEADER_EVENT_TYPE).toString();
        MDC.put(MESSAGE_HEADER_CORRELATION_ID, correlationId);
        MDC.put(MESSAGE_HEADER_EVENT_TYPE, eventType);
        MDC.put("sanity", "check");

        // just print the received message
        log.info("Received the message on manual handler. Expecting MDC to contain correlationId. message: {}",
                body.toString());
    }

    @Incoming("greeting")
    public CompletionStage<Void> consume(Message message) {
        final var unwrap = (IncomingRabbitMQMessage) message.unwrap(IncomingRabbitMQMessage.class);
        MDC.put(MESSAGE_HEADER_CORRELATION_ID, unwrap.getCorrelationId().toString());
        final var buffer = Buffer.buffer((byte[]) unwrap.getPayload());
        log.info("Received the message on microprofile handler. Expecting MDC to contain correlationId. message: {}", buffer);

        return message.ack();
    }

}
