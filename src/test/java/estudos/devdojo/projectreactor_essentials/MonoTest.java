package estudos.devdojo.projectreactor_essentials;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/*
    Reactive Streams
    1. Async
    2. Non-blocking
    3. Backpressure
    
    Publisher <- (subscribe) Subscriber
    Subscription created
    Publisher (onSubscribe with Subscription) -> Subscriber
    Subscription <- manages backpressure (request N) Subscriber
    Publisher -> (onNext) Subscriber
    until:
        1. Publisher sends all requested objects
        2. Publisher sends all objects (onComplete) -> Subscriber and Subscription are cancelled
        3. There is an error (onError) -> Subscriber and Subscription are cancelled
 */
@Slf4j
public class MonoTest {

    private final String name = "Guilherme";
    
    @Test
    void shouldSubscribeToMono() {
        // Mono [0|1]
        // Publisher
        Mono<String> mono = Mono.just(name)
                .log();
        log.info("Mono: {}", mono);

        log.info("Subscribing to mono");
        mono.subscribe();
        System.out.println("-------------------------");
        
        log.info("Testing");
        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    void shouldSubscribeToMono_Consumer() {
        Mono<String> mono = Mono.just(name)
                .log();
        log.info("Mono: {}", mono);

        log.info("Subscribing to mono");
        mono.subscribe(string -> log.info("The string is: {}", string));
        System.out.println("-------------------------");

        log.info("Testing");
        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    void shouldSubscribeToMono_ConsumerError() {
        Mono<String> mono = Mono.just(name)
                .map(string -> {
                    throw new RuntimeException("Error when mapping");
                }); // sync
        
        mono.subscribe(consumer -> log.info("The name is {}", consumer),
                //Throwable::printStackTrace
                throwable -> log.info("An error has ocurred: {}", throwable.getMessage()));

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void shouldSubscribeToMono_ConsumerComplete() {
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(consumer -> log.info("The string is: {}", consumer),
                throwable -> log.info("An error has occurred"),
                () -> log.info("COMPLETED!"));
        
        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    void shouldSubscribeToMono_ConsumerSubscription() {
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(consumer -> log.info("The string is: {}", consumer),
                throwable -> log.info("An error has occurred"),
                () -> log.info("COMPLETED!"),
                Subscription::cancel); // Subscription.cancel() clean resources

        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

}
