@twopubs
Feature: Two event publishers can coexist

  Scenario: An event is published
    Given An event is published
    And there are two publisher instances
    When The event is published
    Then The event is processed once