package com.marklogic.test.suite1.pojo;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.marklogic.client.pojo.annotation.Id;
import com.marklogic.client.pojo.annotation.PathIndexProperty;
import com.marklogic.client.pojo.annotation.PathIndexProperty.ScalarType;


public final class User {

  public String guid;
  private String name;
  private String about;
  private String address;
  private Boolean active;
  private Double balance;
  private Integer age;
  private String gender;
  private Set<Tag> tags;

  public User() {
    super();
    this.tags = new HashSet<Tag>();
  }

  @Id
  public String getGUID() {
    return guid;
  }

  public void setGUID(String guid) {
    this.guid = guid;
  }

  @PathIndexProperty(scalarType = ScalarType.STRING)
  public String getGender() {
    return gender;
  }

  public void setGender(String gender) {
    this.gender = gender;
  }

  @PathIndexProperty(scalarType = ScalarType.DOUBLE)
  public Double getBalance() {
    return balance;
  }

  public void setBalance(Double balance) {
    this.balance = balance;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public Boolean isActive() {
    return active;
  }

  public void setActive(Boolean active) {
    this.active = active;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAbout() {
    return about;
  }

  public void setAbout(String about) {
    this.about = about;
  }

  @PathIndexProperty(scalarType = ScalarType.INT)
  public Integer getAge() {
    return age;
  }

  public void setAge(Integer age) {
    this.age = age;
  }

  public Set<Tag> getTags() {
    return tags;
  }

  public void setTags(Set<Tag> tags) {
    this.tags = tags;
  }

  @Override
  public String toString() {
    return "User [guid=" + guid + ", name=" + name + ", about=" + about
        + ", address=" + address + ", active=" + active + ", balance="
        + balance + ", age=" + age + ", gender=" + gender + ", tags=" + tags
        + "]";
  }

}