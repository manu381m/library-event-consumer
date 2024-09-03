package com.kafka.repository;

import com.kafka.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventRepo extends CrudRepository<LibraryEvent, Integer> {
}
