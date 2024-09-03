package com.kafka.schedule;

import com.kafka.entity.FailureRecord;
import com.kafka.repository.FailureRecordRepo;
import com.kafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailedEventsSchedule {

    private FailureRecordRepo failureRecordRepo;
    private LibraryEventService libraryEventService;

    public FailedEventsSchedule(FailureRecordRepo failureRecordRepo, LibraryEventService libraryEventService) {
        this.failureRecordRepo = failureRecordRepo;
        this.libraryEventService = libraryEventService;
    }

    @Scheduled(fixedRate = 15000)
    public void scheduledRetryEvents() {
        failureRecordRepo.findAllByErrorStatus("retry").forEach(failureRecord -> {
            log.info("Failure event {}", failureRecord);
            var consumerRecord = buildConsumerrecord(failureRecord);
            log.info("Consumer record {}", consumerRecord);
            try {
                //libraryEventService.processLibraryEvent(consumerRecord);
                failureRecord.setErrorStatus("success");
                failureRecordRepo.save(failureRecord);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private ConsumerRecord<Integer, String> buildConsumerrecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(failureRecord.getTopic(), failureRecord.getPartition(), failureRecord.getOffsetRecord(), failureRecord.getRecordKey(), failureRecord.getErrorRecord());
    }
}
