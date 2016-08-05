This command is used to create a caching proxy server in front of the
event store atom feed. Caching is easily applied here as all content
except the most recent feed is immutable.

Dependency:

<pre>
go get github.com/lox/httpcache
go get github.com/alecthomas/kingpin
</pre>

