## FeedRead - Replicate an Event Store by Processing It's Atom Feed

This is an example that shows how to replicate an event store using the event store atom feed.

The first iteration will be a complete copy of the event store, subsequent iterations will pick up
from the last feed read.

## Database Set Up

To replicate the event store in the same database instance, instead of using esdbo and esusr schemeas as
given in the oreeventstore README.md, use different users (replicantdbo and replicantusr) and create
the schema objects etc using those identities.