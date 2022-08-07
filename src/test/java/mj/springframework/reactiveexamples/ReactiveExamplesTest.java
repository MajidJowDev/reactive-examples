package mj.springframework.reactiveexamples;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ReactiveExamplesTest {

    Person michael = new Person("Michael", "Weston");
    Person fiona = new Person("Fiona", "Glenanne");
    Person sam = new Person("Sam", "Axe");
    Person jesse = new Person("Jesse", "Porter");

    @Test
    public void monoTests() throws Exception {
        //create new person mono
        Mono<Person> personMono = Mono.just(michael);  // just creates a Mono Object from the given obj

        //get person object from mono publisher
        Person person = personMono.block();

        // output name
        log.info(person.sayMyName());
    }

    @Test
    public void monoTransform() throws Exception {
        //create new person mono
        Mono<Person> personMono = Mono.just(fiona);

        PersonCommand command = personMono
                .map(person -> { //type transformation
                    return new PersonCommand(person);
                }).block(); // the code won't be executed until something blocks on it

        log.info(command.sayMyName());
    }

    @Test//(expected = NullPointerException.class)
    public void monoFilter() throws Exception {

        // JUnit 5 (Jupiter) does not have the expected param for @Test.. so we need to write our Test code like follow

        NullPointerException thrown = Assertions.assertThrows(NullPointerException.class, () -> {
            //Code under test
            Mono<Person> personMono = Mono.just(sam);

            //filter example
            Person samAxe = personMono
                    .filter(person -> person.getFirstName().equalsIgnoreCase("foo"))
                    .block();

            log.info(samAxe.sayMyName()); //throws NullPointerException
        });

       // log.info("thrown exception stat: " + thrown.getMessage());

    }

    @Test
    public void fluxTest() throws Exception {

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.subscribe(person -> log.info(person.sayMyName()));

    }

    @Test
    public void fluxTestFilter() throws Exception {

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.filter(person -> person.getFirstName().equals(fiona.getFirstName()))
                .subscribe(person -> log.info(person.sayMyName()));

    }

    @Test
    public void fluxTestDelayNoOutput() throws Exception {

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.delayElements(Duration.ofSeconds(1))
                .subscribe(person -> log.info(person.sayMyName()));

    }

    @Test
    public void fluxTestDelay() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.delayElements(Duration.ofSeconds(1))
                .doOnComplete(countDownLatch::countDown)
                .subscribe(person -> log.info(person.sayMyName()));

        countDownLatch.await(); // this line tells the test to wait until the count down is complete

    }

    @Test
    public void fluxTestFilterDelay() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.delayElements(Duration.ofSeconds(1))
                .filter(person -> person.getFirstName().contains("i"))
                .doOnComplete(countDownLatch::countDown)
                .subscribe(person -> log.info(person.sayMyName()));

        countDownLatch.await();
    }

}