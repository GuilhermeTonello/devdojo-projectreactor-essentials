package estudos.devdojo.projectreactor_essentials;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class OperatorsTest {

    @BeforeAll
    static void beforeAll() {
        BlockHound.install();
    }

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
    void subscribeOn_IO()  {
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

    @Test
    void switchIfEmptyOperator() {
        Flux<Object> flux = Flux.empty()
                .switchIfEmpty(Flux.just("not empty anymore"))
                .log();

        StepVerifier.create(flux)
                .expectNext("not empty anymore")
                .verifyComplete();
    }

    @Test
    void deferOperator() throws Exception {
        Mono<Long> mono = Mono.just(System.currentTimeMillis()); // Don't change values
        
        mono.subscribe(aLong -> log.info("{}", aLong));
        Thread.sleep(100L);
        mono.subscribe(aLong -> log.info("{}", aLong));

        System.out.println();
        
        Mono<Long> monoDefer = Mono.defer(() -> Mono.just(System.currentTimeMillis())); // Executes Mono.just everytime it's subscribed
        monoDefer.subscribe(aLong -> log.info("{}", aLong));
        Thread.sleep(100L);
        monoDefer.subscribe(aLong -> log.info("{}", aLong));

        AtomicLong atomicLong = new AtomicLong();
        monoDefer.subscribe(atomicLong::set);
        assertTrue(atomicLong.get() >= 0);
    }
    
    @Test
    void concatOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> fluxConcat = Flux.concat(flux1, flux2)
                .log();
        
        StepVerifier.create(fluxConcat)
                .expectNext("a", "b", "c", "d")
                .verifyComplete();
    }

    @Test
    void concatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> fluxConcatWith = flux1.concatWith(flux2);

        StepVerifier.create(fluxConcatWith)
                .expectNext("a", "b", "c", "d")
                .verifyComplete();
    }

    @Test
    void combineLatestOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> fluxCombineLatest = Flux.combineLatest(flux1, flux2, 
                        (f1, f2) -> f1.toUpperCase() + "_" + f2.toUpperCase())
                        .log();

        StepVerifier.create(fluxCombineLatest)
                .expectNext("B_C", "B_D") // Can't really know the order here
                .verifyComplete();
    }

    @Test
    void mergeOperator() throws Exception {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(100));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merge = Flux.merge(flux1, flux2) // Merge is eager, run on parallel Threads and don't wait for one of them to complete
                .log();

        merge.subscribe(log::info);
        
        Thread.sleep(300L);

        StepVerifier.create(merge)
                .expectNext("a", "b", "c", "d")
                .expectComplete();
    }

    @Test
    void mergeWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merge = flux1.mergeWith(flux2)
                .log();

        merge.subscribe(log::info);

        StepVerifier.create(merge)
                .expectNext("a", "b", "c", "d")
                .expectComplete();
    }

    @Test
    void mergeSequentialOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merge = Flux.mergeSequential(flux1, flux2, flux1)
                .log();

        merge.subscribe(log::info);

        StepVerifier.create(merge)
                .expectNext("a", "b", "c", "d", "a", "b")
                .expectComplete();
    }

    @Test
    void concatOperatorWithError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new RuntimeException("Runtime error");
                    }
                    return s;
                });
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> fluxConcat = Flux.concatDelayError(flux1, flux2) // Continues concat if an error occurs and throw error at end
                .log();

        StepVerifier.create(fluxConcat)
                .expectNext("a", "c", "d")
                .expectError()
                .verify();
    }

    @Test
    void mergeOperatorWithError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new RuntimeException("Runtime error");
                    }
                    return s;
                })
                .doOnError(throwable -> {
                    log.info("Some random error occurred");
                });
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> fluxMerge = Flux.mergeDelayError(1, flux1, flux2, flux1) // prefetch
                .log();

        StepVerifier.create(fluxMerge)
                .expectNext("a", "c", "d", "a")
                .expectError()
                .verify();
    }
    
    @Test
    void flatMapOperator() throws Exception {
        Flux<String> flux = Flux.just("a", "b");

//        Flux<Flux<String>> fluxFlux = flux.map(String::toUpperCase)
//                .map(this::findByName)
//                .log();

        Flux<String> fluxString = flux.map(String::toUpperCase)
                .flatMap(this::findByName) // flatMap is async, don't preserve order
                .log();

        fluxString.subscribe(stringFlux -> log.info("{}", stringFlux));
        
        Thread.sleep(500L);
        
        StepVerifier.create(fluxString)
                .expectNext("nameB1", "nameB2", "nameA1", "nameA2")
                .verifyComplete();
    }

    @Test
    void flatMapSequentialOperator() throws Exception {
        Flux<String> flux = Flux.just("a", "b");

//        Flux<Flux<String>> fluxFlux = flux.map(String::toUpperCase)
//                .map(this::findByName)
//                .log();

        Flux<String> fluxString = flux.map(String::toUpperCase)
                .flatMapSequential(this::findByName) // Maintain order
                .log();

        fluxString.subscribe(stringFlux -> log.info("{}", stringFlux));

        Thread.sleep(500L);

        StepVerifier.create(fluxString)
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .verifyComplete();
    }
    
    @Test
    void zipOperator() {
        Flux<String> titleFlux = Flux.just("Grand Blue", "Baki");
        Flux<String> studioFlux = Flux.just("Zero-G", "TMS Entertainment");
        Flux<Integer> episodesFlux = Flux.just(12, 24);

        Flux<Anime> animeFlux = Flux.zip(titleFlux, studioFlux, episodesFlux)
                .log()
                .flatMap(objects -> Flux.just(new Anime(objects.getT1(), objects.getT2(), objects.getT3())));
        
        animeFlux.subscribe(anime -> log.info("{}", anime));
        
        StepVerifier.create(animeFlux)
                .expectNext(new Anime("Grand Blue", "Zero-G", 12))
                .expectNext(new Anime("Baki", "TMS Entertainment", 24))
                .verifyComplete();
    }

    @Test
    void zipWithOperator() {
        Flux<String> titleFlux = Flux.just("Grand Blue", "Baki");
        Flux<String> studioFlux = Flux.just("Zero-G", "TMS Entertainment");
        Flux<Integer> episodesFlux = Flux.just(12, 24);

        Flux<Anime> animeFlux = titleFlux.zipWith(episodesFlux)
                .log()
                .flatMap(objects -> Flux.just(new Anime(objects.getT1(), null, objects.getT2())));

        animeFlux.subscribe(anime -> log.info("{}", anime));

        StepVerifier.create(animeFlux)
                .expectNext(new Anime("Grand Blue", null, 12))
                .expectNext(new Anime("Baki", null, 24))
                .verifyComplete();
    }
    
    private record Anime(String name, String studio, Integer episodes) {
        @Override
        public String toString() {
            return String.format("{ name: %s, studio: %s, episodes: %d }", name, studio, episodes);
        }
    }
    
    private Flux<String> findByName(String name) {
        return name.equals("A")
                ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(200L))
                : Flux.just("nameB1", "nameB2");
    }

}
