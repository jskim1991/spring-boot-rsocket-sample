package io.jay.requester;

import io.rsocket.metadata.WellKnownMimeType;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.config.annotation.rsocket.RSocketSecurity;
import org.springframework.security.rsocket.core.PayloadSocketAcceptorInterceptor;
import org.springframework.security.rsocket.metadata.SimpleAuthenticationEncoder;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SpringBootApplication
public class RequesterApplication {

    @SneakyThrows
    public static void main(String[] args) {
        SpringApplication.run(RequesterApplication.class, args);
    }
}

@Configuration
class SecurityConfiguration {
    @Bean
    PayloadSocketAcceptorInterceptor payloadSocketAcceptorInterceptor(RSocketSecurity security) {
        return security.authorizePayload(authorize ->
                        authorize.anyRequest().permitAll())
                .build();

    }
}

@Configuration
class RequesterConfiguration {

    @Value("${rsocket.port:8181}")
    private int port;

    @Bean
    RSocketRequester rSocketRequester(RSocketRequester.Builder builder, RSocketMessageHandler handler) {
        return builder
                .rsocketStrategies(b -> b.encoder(new SimpleAuthenticationEncoder()))
                /* NOTE: Authentication can be configured on connection setup as well
                .setupRoute("connect")
                .setupData("")
                .setupMetadata(new UsernamePasswordMetadata("jay", "pw"), MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString()))
                */
                .rsocketConnector(connector -> connector.acceptor(handler.responder()))
                .tcp("localhost", port);
    }
}

@Log4j2
@RestController
@RequiredArgsConstructor
class RequesterController {

    private final RSocketRequester rSocketRequester;

    @GetMapping("/request-response")
    public Mono<String> sendRequestResponse() {
        log.info("Sending request / response");
        return rSocketRequester.route("responder-request-response.{id}", 123)
                .metadata("some-custom-header-value", MimeType.valueOf("messaging/custom-header"))
                .data("Reactive Spring")
                .retrieveMono(String.class);
    }

    @GetMapping("/auth")
    public Mono<String> sendSimpleAuthentication() {
        return rSocketRequester.route("auth")
                .metadata(new UsernamePasswordMetadata("jay", "pw"), MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString()))
                .data(Mono.empty())
                .retrieveMono(String.class);
    }

    @GetMapping(value = "/channel-stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> sendStream() {
        log.info("Sending channel stream ...");
        Flux<String> data = Flux.interval(Duration.ofSeconds(1))
                .take(10)
                .map(i -> UUID.randomUUID().toString());
        return rSocketRequester.route("responder-channel-stream")
                .data(data)
                .retrieveFlux(String.class)
                .doOnNext(log::info);
    }

    @GetMapping("/fire-forget")
    public void sendFireAndForget() {
        log.info("Sending fire-and-forget");
        rSocketRequester.route("responder-fire-forget")
                .data(UUID.randomUUID().toString())
                .send()
                .subscribe();
    }

    @GetMapping("/channel-bidirectional")
    public void sendBidirectional() {
        /* NOTE: multiple clients can be configured with below
        int max = Math.max(5, (int) (Math.random() * 10));
         */
        int max = 1;
        log.info("Launching " + max + " clients");
        Flux.fromStream(IntStream.range(0, max).boxed())
                .delayElements(Duration.ofSeconds(1))
                .map(id -> new Requester(rSocketRequester, id.toString()))
                .flatMap(Requester::getGreetings)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(log::info);
    }

    @GetMapping("/error")
    public Mono<String> causeError() {
        return rSocketRequester.route("error")
                .data(UUID.randomUUID().toString())
                .retrieveMono(String.class)
                .doOnError(log::error)
                .onErrorReturn("error returned from service");
    }
}

@Log4j2
@Controller
class RequesterMessageController {

    @MessageMapping("health")
    Flux<RequesterHealthState> health() {
        var start = new Date().getTime();
        var delay = Duration.ofSeconds(3).toMillis();
        return Flux.fromStream(Stream.generate(() -> {
                    var now = new Date().getTime();
                    var stop = (start + delay) < now;
                    return new RequesterHealthState(stop ? RequesterHealthState.STOPPED : RequesterHealthState.STARTED);
                }))
                .delayElements(Duration.ofSeconds(1))
                .doOnNext(chs -> log.info("Sending status " + chs.getState()));
    }
}

@Log4j2
@RequiredArgsConstructor
class Requester {
    private final RSocketRequester rSocketRequester;
    private final String id;

    Flux<String> getGreetings() {
        return rSocketRequester.route("responder-channel-bidirectional")
                .data("Client #" + id)
                .retrieveFlux(String.class);
    }
}