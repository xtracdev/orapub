## Feed Records Sample

This command shows how to use the publisher to build a CQRS query view based on event publishing. The basic idea
is we take the event publishing data and write it out in a way that supports querying for building hourly feeds.

This will assume the user set up as detailed in the oraeventstore project, e.g. esdbo and esusr - see the
oraeventstore README.md for details.

Note this sample assumes a single instance of this code is running, if multiple instances were deployed and active
then some sort of leader election/coordination of who gets to run would be required.

Here's how it works.


In a loop:

1. Read the feed_state table and compare it with the current time - if the current time
is within the feed state record, move on. If not, create a new entry in the feeds table (use the current feedid as
the previous entry in the feeds table, and update the feed state).

2. Read the published events.
3. For each published event, retrieve its details, and write an entry into the feed data table.

Note the important thing here is the partitioning of data into feeds, and the order withing the feeds, not the
absolute capturing of event timestamps.

Tables:

<pre>
create table feed_state (
    feedid varchar2(100) not null,
    year integer not null,
    month integer not null,
    day integer not null,
    hour integer not null
);

create table feeds (
    id  number generated always as identity,
    feedid varchar2(100) not null,
    previous varchar2(100)
)

create table feed_data (
    id number generated always as identity,
    feedid varchar2(100) not null,
    event_time timestamp DEFAULT current_timestamp,
    aggregate_id varchar2(60)not null,
    version integer not null,
    typecode varchar2(30) not null,
     payload blob
)

create or replace synonym esusr.feed_state for esdbo.feed_state;
create or replace synonym esusr.feeds for esdbo.feeds;
create or replace synonym esusr.feed_data for esdbo.feed_data;

grant insert, delete, select on feed_state to esusr;
grant insert, select on feeds to esusr;
grant insert, select on feed_data to esusr;

</pre>

