package estudos.devdojo.projectreactor_essentials;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
class FluxTest {

    @BeforeAll
    static void beforeAll() {
        BlockHound.install();
    }
    
    private String[] strings = {
            "Silvio", "Santos", "TV", "SBT", "Olha o aviãzinho"
    };
    
    @Test
    void shouldSubscribeToFlux() {
        Flux<String> flux = Flux.just(strings)
                .log();

        StepVerifier.create(flux)
                .expectNext(strings)
                .verifyComplete();
    }

    @Test
    void shouldSubscribeToFluxOfNumbers() {
        Flux<Integer> flux = Flux.range(1, 5)
                .log();

        flux.subscribe(integer -> log.info("Number: {}", integer));
        
        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void shouldSubscribeToFluxFromList() {
        Flux<Integer> flux = Flux.fromIterable(List.of(1, 2, 3))
                .log();

        flux.subscribe(integer -> log.info("Number: {}", integer));

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .verifyComplete();
    }

    @Test
    void shouldSubscribeToFluxError() {
        Flux<Integer> flux = Flux.range(1, 5)
                .log()
                .map(integer -> {
                    if (integer == 4)
                        throw new RuntimeException("i é igual a 4");
                    return integer;
                });

        flux.subscribe(integer -> log.info("Number: {}", integer),
                throwable -> log.info("Error: {}", throwable.getMessage()),
                () -> log.info("COMPLETED!"),
                subscription -> subscription.request(3));

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void shouldSubscribeToFluxUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
                .log();

        flux.subscribe(new Subscriber<Integer>() {
            private int count = 0;
            private Subscription subscription;
            private int requestCount = 2;
            
            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(requestCount);
            }

            @Override
            public void onNext(Integer integer) {
                this.count++;
                if (count >= requestCount) {
                    this.count = 0;
                    subscription.request(requestCount);
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    void shouldSubscribeToFluxNotSoUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
                .log();

        flux.subscribe(new BaseSubscriber<Integer>() {
            private int count = 0;
            private final int requestCount = 2;
            
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    void shouldSubscribeToFluxPrettyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
                .log()
                .limitRate(3); // If above log -> request(unbounded)

        flux.subscribe(integer -> log.info("Number: {}", integer));
        
        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    void shouldSubscribeToFlux_Interval_V1() throws Exception {
        Flux<Long> flux = Flux.interval(Duration.ofMillis(100L))
                .take(5)
                .log();
        
        flux.subscribe(aLong -> log.info("Number: {}", aLong));
        
        Thread.sleep(3_000L);
    }

    @Test
    void shouldSubscribeToFlux_Interval_V2() {
        StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofDays(1))
                        .log())
                .expectSubscription()
                .expectNoEvent(Duration.ofDays(1))
                .thenAwait(Duration.ofDays(2))
                .expectNext(0L, 1L)
                .thenCancel()
                .verify();
    }

    @Test
    void shouldSubscribeToFlux_Interval_V2P1() {
        StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofDays(1))
                        .log())
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(24))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }
    
    @Test
    void connectableFlux() throws Exception {
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100L))
                .publish(); // Create connectable flux
        
//        connectableFlux.connect();

//        log.info("Fim connectable flux");        
//        Thread.sleep(300L);
//        
//        connectableFlux.subscribe(integer -> log.info("Number: {}", integer));

        StepVerifier.create(connectableFlux)
                .then(connectableFlux::connect)
                .thenConsumeWhile(integer -> integer <= 5)
                .expectNext(6, 7, 8, 9, 10)
                .expectComplete()
                .verify();
    }

    @Test
    void connectableFlux_autoConnect() throws Exception {
        Flux<Integer> connectableFlux = Flux.range(1, 5)
                .log()
                .delayElements(Duration.ofMillis(100L))
                .publish()
                .autoConnect(2); // Create connectable flux

        StepVerifier.create(connectableFlux)
                .then(connectableFlux::subscribe)
                .expectNext(1, 2, 3, 4, 5)
                .expectComplete()
                .verify();
    }
    
}
