package io.jay.responder;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.rsocket.messaging.RSocketStrategiesCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.codec.StringDecoder;
import org.springframework.messaging.handler.annotation.*;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.ConnectMapping;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableReactiveMethodSecurity;
import org.springframework.security.config.annotation.rsocket.EnableRSocketSecurity;
import org.springframework.security.config.annotation.rsocket.RSocketSecurity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.messaging.handler.invocation.reactive.AuthenticationPrincipalArgumentResolver;
import org.springframework.security.rsocket.core.PayloadSocketAcceptorInterceptor;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

@SpringBootApplication
public class ResponderApplication {

    @SneakyThrows
    public static void main(String[] args) {
        SpringApplication.run(ResponderApplication.class, args);
    }
}

class Constants {
    public static final String CUSTOM_HEADER = "custom-header";
    public static final MimeType CUSTOM_HEADER_MIMETYPE = MimeType.valueOf("messaging/" + CUSTOM_HEADER);
}

@Configuration
class ResponderConfiguration {
    @Bean
    RSocketStrategiesCustomizer rSocketStrategiesCustomizer() {
        return strategies -> strategies
                .metadataExtractorRegistry(registry -> {
                    registry.metadataToExtract(Constants.CUSTOM_HEADER_MIMETYPE, String.class, Constants.CUSTOM_HEADER);
                })
                .decoders(decoders -> decoders.add(StringDecoder.allMimeTypes()));
    }
}

@Log4j2
@Controller
class ResponderController {

    @MessageMapping("responder-request-response.{id}")
    public Mono<String> handleRequestResponse(@DestinationVariable Integer id,
                                       @Header(Constants.CUSTOM_HEADER) String customHeader,
                                       @Headers Map<String, Object> metadata,
                                       @Payload String payload) {
        log.info("Request / Response");
        log.info("Custom header value is " + customHeader);
        return Mono.just("Hello " + payload + " with id of " + id)
                .doOnNext(log::info);
    }

    @MessageMapping("responder-channel-stream")
    public Flux<String> handleStream(@Payload Flux<String> payloads) {
        log.info("Channel Stream");
        return payloads.map(request -> request.toUpperCase(Locale.ROOT))
                .doOnNext(log::info);
    }

    @MessageMapping("responder-fire-forget")
    public void handleFireAndForget(String payload) {
        log.info("Fire and Forget");
        log.info(payload);
    }

    @MessageMapping("responder-channel-bidirectional")
    public Flux<String> handleBidirectional(RSocketRequester client, @Payload String request) {
        log.info("Bi-directional " + request);
        Flux<ConditionFlag> healthFlux = client.route("health")
                .data(Mono.just("STARTED?"))
                .retrieveFlux(ConditionFlag.class)
                .filter(chs -> chs.getState().equalsIgnoreCase(ConditionFlag.STOPPED))
                .doOnNext(chs -> log.info(chs.toString()));

        Flux<String> replyPayload = Flux.fromStream(Stream.generate(() -> ("Hello " + request + " @ " + Instant.now())))
                .delayElements(Duration.ofSeconds(1));
        return replyPayload.takeUntilOther(healthFlux)
                .doOnNext(log::info);
    }

    @MessageMapping("error")
    public Mono<String> error() {
        return Mono.error(new RuntimeException("Something bad happened"));
    }

    @MessageExceptionHandler(RuntimeException.class)
    public Mono<RuntimeException> exceptionHandler(RuntimeException runtimeException) {
        log.error(runtimeException.getMessage());
        return Mono.error(runtimeException);
    }
}

@Configuration
@EnableRSocketSecurity
@EnableReactiveMethodSecurity
class SecurityConfiguration {

    @Bean
    MapReactiveUserDetailsService authentication() {
        return new MapReactiveUserDetailsService(
                User.builder()
                        .username("jay")
                        .password("{noop}pw")
                        .roles("ADMIN", "USER")
                        .build());
    }

    @Bean
    PayloadSocketAcceptorInterceptor authorization(RSocketSecurity security) {
        return security
                .authorizePayload(authorize ->
                        authorize.route("auth").authenticated()
                                .anyRequest().permitAll()
                                .anyExchange().permitAll()
                )
                .simpleAuthentication(Customizer.withDefaults())
                .build();
    }

    @Bean
    RSocketMessageHandler rSocketMessageHandler(RSocketStrategies strategies) {
        RSocketMessageHandler handler = new RSocketMessageHandler();
        handler.getArgumentResolverConfigurer()
                .addCustomResolver(new AuthenticationPrincipalArgumentResolver());
        handler.setRSocketStrategies(strategies);
        return handler;
    }
}

@Log4j2
@Controller
class RestrictedController {

    @ConnectMapping("connect")
    public void connect(@AuthenticationPrincipal Mono<UserDetails> user) {
        user.map(UserDetails::getUsername)
                .subscribe(name -> log.info("Connection established for " + name));
    }

    @MessageMapping("auth")
    public Mono<String> authenticate(@AuthenticationPrincipal Mono<UserDetails> user) {
        return user.map(u -> "Hi there " + u.getUsername());
    }
}