package com.kafka.config;

import com.kafka.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;
import java.util.Objects;

@Configuration
@Slf4j
//@EnableKafka
public class kafkaConfig {

    private static final String RETRY = "retry";
    private static final String DEAD = "dead";

    private KafkaProperties properties;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private FailureService failureService;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String dltTopic;

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (consumerRecord, e) -> {
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, consumerRecord.partition());
            } else {
                return new TopicPartition(dltTopic, consumerRecord.partition());
            }
        });
    }

    ConsumerRecordRecoverer recordRecoverer = (consumerRecord, e) -> {
        log.info("Publishing Consumer Record Recoverer");
        var record = (ConsumerRecord<Integer, String>)consumerRecord;
        if (e.getCause() instanceof RecoverableDataAccessException) {
            log.info("From Consumer record recoverer RETRY");
            failureService.saveFailureRecord(record,e, RETRY);
        }
        else {
            log.info("From Consumer record recoverer DEAD");
            failureService.saveFailureRecord(record,e,DEAD);
        }
    };

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory, ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer, ObjectProvider<SslBundles> sslBundles) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory) kafkaConsumerFactory.getIfAvailable(() -> {
            return new DefaultKafkaConsumerFactory(this.properties.buildConsumerProperties((SslBundles) sslBundles.getIfAvailable()));
        }));
        Objects.requireNonNull(factory);
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        //factory.setConcurrency(3);
        factory.setCommonErrorHandler(commonErrorHandler());
        return factory;
    }

    private DefaultErrorHandler commonErrorHandler() {
        var fixedBackOff = new FixedBackOff(1000L, 3);
        var expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1000L);
        expBackOff.setMultiplier(1.5);
        expBackOff.setMaxInterval(2000L);
        var errorHandler = new DefaultErrorHandler(
                //publishingRecoverer(),
                //fixedBackOff,
                recordRecoverer,
                expBackOff);
        var exceptionsIgnoresList = List.of(IllegalArgumentException.class);
        var exceptionsRetryableList = List.of(RecoverableDataAccessException.class);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener Record: {}, Error message: {}, deliveryAttempt: {}"
                    , record, ex.getMessage(), deliveryAttempt);
        });
        exceptionsIgnoresList.forEach(errorHandler::addNotRetryableExceptions);
        exceptionsRetryableList.forEach(errorHandler::addRetryableExceptions);
        return errorHandler;
    }


}
