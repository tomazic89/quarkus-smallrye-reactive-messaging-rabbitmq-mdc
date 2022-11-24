package org.acme.model;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;

import io.vertx.core.buffer.Buffer;

public record AmqpMessage(byte[] body, AMQP.BasicProperties properties, String eventType) {
    private String getStringHeader(BasicProperties props, String name) {
        var r = props.getHeaders().get(name);
        if (r != null) {
            return r.toString();
        }
        return null;
    }
}
