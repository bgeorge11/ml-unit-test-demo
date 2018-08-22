package com.marklogic.test.suite1.springdata;

import java.util.List;

import io.github.malteseduck.springframework.data.marklogic.repository.MarkLogicRepository;

public interface PersonRepository extends MarkLogicRepository<Person, String> {
    List<Person> findByLastname(String lastname);
}