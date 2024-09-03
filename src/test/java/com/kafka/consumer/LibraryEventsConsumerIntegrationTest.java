package com.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.Book;
import com.kafka.entity.LibraryEvent;
import com.kafka.entity.LibraryEventType;
import com.kafka.repository.FailureRecordRepo;
import com.kafka.repository.LibraryEventRepo;
import com.kafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}", "retryListener.startup:false"})
@Slf4j
class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker broker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    ObjectMapper objectMapper;

    @SpyBean
    LibraryEventsConsumer consumerSpy;

    @SpyBean
    LibraryEventService serviceSpy;

    @SpyBean
    LibraryEventRepo repoSpy;

    @Autowired
    FailureRecordRepo failureRecordRepo;

    private Consumer<Integer, String> consumer;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String dltTopic;

    @BeforeEach
    void setUp() {
        var container = kafkaListenerEndpointRegistry.getListenerContainers().stream().filter(messageListenerContainer ->
            //messageListenerContainer.getGroupId().equals("library-events-consumer-group");
            Objects.equals(messageListenerContainer.getGroupId(), "library-events-consumer-group")
        ).collect(Collectors.toList()).get(0);
        ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());
        /*for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, broker.getPartitionsPerTopic());
        }*/
    }

    @AfterEach
    void tearDown() {
        repoSpy.deleteAll();
    }

    @Test
    void listen() throws InterruptedException, ExecutionException, JsonProcessingException {

        var json = "{\"libraryEventId\": null,\"libraryEventType\": \"NEW\",\"book\": {\"bookId\": 123,\"bookName\": \"Kafka Using Spring Boot\",\"bookAuthor\": \"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(consumerSpy, times(1)).listen(isA(ConsumerRecord.class));
        verify(serviceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) repoSpy.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(123, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void listenUpdate() throws JsonProcessingException, ExecutionException, InterruptedException {
        var json = "{\"libraryEventId\": null,\"libraryEventType\": \"NEW\",\"book\": {\"bookId\": 123,\"bookName\": \"Kafka Using Spring Boot\",\"bookAuthor\": \"Dilip\"}}";
        var libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repoSpy.save(libraryEvent);

        var updatedBook = Book.builder().bookId(123).bookName("Kafka Using Spring Boot 2.x").bookAuthor("Dilip").build();
        libraryEvent.setBook(updatedBook);
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        LibraryEvent persistedLibraryEvent = repoSpy.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot 2.x", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void listenUpdateWithNullId() throws JsonProcessingException, ExecutionException, InterruptedException {
        var json = "{\"libraryEventId\": null,\"libraryEventType\": \"UPDATE\",\"book\": {\"bookId\": 123,\"bookName\": \"Kafka Using Spring Boot\",\"bookAuthor\": \"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(4, TimeUnit.SECONDS);

        verify(consumerSpy, times(1)).listen(isA(ConsumerRecord.class));
        verify(serviceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void listenUpdateWithNullIdWithRecoverer() throws JsonProcessingException, ExecutionException, InterruptedException {
        var json = "{\"libraryEventId\": null,\"libraryEventType\": \"UPDATE\",\"book\": {\"bookId\": 123,\"bookName\": \"Kafka Using Spring Boot\",\"bookAuthor\": \"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(4, TimeUnit.SECONDS);

        verify(consumerSpy, times(1)).listen(isA(ConsumerRecord.class));
        verify(serviceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        var configs = new HashMap<>(KafkaTestUtils.consumerProps("group2", "true", broker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, dltTopic);

        var consumerRecord = KafkaTestUtils.getSingleRecord(consumer, dltTopic);

        assertEquals(json, consumerRecord.value());
    }

    @Test
    void listenUpdateWith_999IdWithRecoverer() throws JsonProcessingException, ExecutionException, InterruptedException {
        var json = "{\"libraryEventId\": 999,\"libraryEventType\": \"UPDATE\",\"book\": {\"bookId\": 123,\"bookName\": \"Kafka Using Spring Boot\",\"bookAuthor\": \"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(consumerSpy, times(3)).listen(isA(ConsumerRecord.class));
        verify(serviceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", broker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        var consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);

        assertEquals(json, consumerRecord.value());

    }

    @Test
    void listenUpdateWithNullIdRecordRecoverer() throws JsonProcessingException, ExecutionException, InterruptedException {
        var json = "{\"libraryEventId\": null,\"libraryEventType\": \"UPDATE\",\"book\": {\"bookId\": 123,\"bookName\": \"Kafka Using Spring Boot\",\"bookAuthor\": \"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(4, TimeUnit.SECONDS);

        verify(consumerSpy, times(1)).listen(isA(ConsumerRecord.class));
        verify(serviceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        var count = failureRecordRepo.count();
        assertEquals(1, count);

    }
}