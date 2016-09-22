@pubreader
Feature: Processing events in the publish table
  Scenario: Events ready to publish
    Given Some freshly stored events
    When The publish table is polled for events
    Then The freshly stored events are returned
    And the event details can be retrieved
    And published events can be removed from the publish table
