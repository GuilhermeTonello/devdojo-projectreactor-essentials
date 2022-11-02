package estudos.devdojo.projectreactor_essentials;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

@Slf4j
class OperatorsTest {

    @Test
    void subscribeOnSingle() {
        Flux<Integer> integerFlux = Flux.range(1, 5)
                .map(integer -> {
                    log.info("MAP 1 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .subscribeOn(Schedulers.single()) // Affects everything, after and before subscribeOn
                .map(integer -> {
                    log.info("MAP 2 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(integerFlux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void pubishOnSimple() {
        Flux<Integer> integerFlux = Flux.range(1, 5)
                .map(integer -> {
                    log.info("MAP 1 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .publishOn(Schedulers.boundedElastic()) // Affects only after publishOn
                .map(integer -> {
                    log.info("MAP 2 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                });
        
        StepVerifier.create(integerFlux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void multipleSubscribeOn() {
        Flux<Integer> integerFlux = Flux.range(1, 5)
                .subscribeOn(Schedulers.single()) // First declared subscribeOn will be used
                .map(integer -> {
                    log.info("MAP 1 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(integer -> {
                    log.info("MAP 2 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(integerFlux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void multiplePublishOn() {
        Flux<Integer> integerFlux = Flux.range(1, 5)
                .publishOn(Schedulers.single())
                .map(integer -> {
                    log.info("MAP 1 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(integer -> {
                    log.info("MAP 2 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(integerFlux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void subscribeOnAndublishOn_V1() {
        Flux<Integer> integerFlux = Flux.range(1, 5)
                .publishOn(Schedulers.single()) // All will be "single"
                .map(integer -> {
                    log.info("MAP 1 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(integer -> {
                    log.info("MAP 2 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(integerFlux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void subscribeOnAndublishOn_V2() {
        Flux<Integer> integerFlux = Flux.range(1, 5)
                .subscribeOn(Schedulers.boundedElastic())
                .map(integer -> {
                    log.info("MAP 1 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                })
                .publishOn(Schedulers.single()) // Only MAP 2 will be "single"
                .map(integer -> {
                    log.info("MAP 2 -> Number: {}, Thread: {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(integerFlux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }
    
    @Test
    void subscribeOn_IO() {
        Mono<List<String>> mono = Mono.fromCallable(() -> Files.readAllLines(Path.of("my-file.txt"))) // fromCallable() Executes tasks that blocks the Thread on another Thread
                .log()
                .subscribeOn(Schedulers.boundedElastic());

//        mono.subscribe(strings -> log.info("{}", strings));
        
        StepVerifier.create(mono)
                .thenConsumeWhile(strings -> {
                    assertFalse(strings.isEmpty());
                    return true;
                })
                .verifyComplete();
    }

}
