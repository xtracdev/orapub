@regpub
Feature: All registered publishers are called when an event is processed

  Scenario: An event is processed
    Given An event to be published
    And Some registered event processors
    When The publisher processes the event
    Then All the registered event processors are invoked with the event