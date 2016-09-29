# OraPub

[![CircleCI](https://circleci.com/gh/xtracdev/orapub.svg?style=svg)](https://circleci.com/gh/xtracdev/orapub)

This project provides code to process events to publish in the Oracle
event store, with hooks for adding processing functions to be invoked
for each published event.

## Dependencies

<pre>
go get github.com/Sirupsen/logrus
go get github.com/xtracdev/goes
go get github.com/rjeczalik/pkgconfig/cmd/pkg-config
go get github.com/mattn/go-oci8
</pre>

## Contributing

To contribute, you must certify you agree with the [Developer Certificate of Origin](http://developercertificate.org/)
by signing your commits via `git -s`. To create a signature, configure your user name and email address in git.
Sign with your real name, do not use pseudonyms or submit anonymous commits.


In terms of workflow:

0. For significant changes or improvement, create an issue before commencing work.
1. Fork the respository, and create a branch for your edits.
2. Add tests that cover your changes, unit tests for smaller changes, acceptance test
for more significant functionality.
3. Run gofmt on each file you change before committing your changes.
4. Run golint on each file you change before committing your changes.
5. Make sure all the tests pass before committing your changes.
6. Commit your changes and issue a pull request.

## License

(c) 2016 Fidelity Investments
Licensed under the Apache License, Version 2.0

### TODO

* Move event polling, processing, and deleting inside orapub, not in es-data-pub.
* PollEvents, DeleteProcessedEvents, and RetrieveEventDetail become unexported