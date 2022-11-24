package org.acme.resources;

import static org.acme.services.GreetingService.MESSAGE_HEADER_CORRELATION_ID;

import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.acme.services.GreetingService;
import org.slf4j.MDC;

@Path("/hello")
public class GreetingResource {

    @Inject
    GreetingService service;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "Hello from RESTEasy Reactive";
    }

    @POST
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)
    public Response sendHello(String message) {
        final var correlationId = UUID.randomUUID().toString();
        MDC.put(MESSAGE_HEADER_CORRELATION_ID, correlationId);
        service.sendToExchange(correlationId, message);
        return Response.ok("Message sent to exchange.").build();
    }
}