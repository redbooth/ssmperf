ssmperf
=======

ssmperf is a basic load testing tool for servers implementing the
[Stupid-Simple Messaging Protocol](https://github.com/aerofs/ssmp).

License
-------

BSD 3-clause, see accompanying LICENSE file.


Dependencies
------------

Required:
  - [Go](https://golang.org) 1.4+


Usage
-----

```
./ssmperf <address> [flags]
  -cacert=""        path to CA cert
  -cert=""          path to client cert
  -conn=100         number of client connections
  -count=10000      number of messages sent per connection
  -cpuprofile=""    write cpu profile to file
  -insecure=false   disable TLS
  -key=""           path to client key
  -memprofile=""    write memory profile to this file
  -secret=""        shared secret to use for auth, default to open login if empty
  -size=100         payload size in bytes [16, 980]
  -sub=10           number of subscribers per topic
  -type="UCAST"     message type (UCAST, MCAST, or BCAST)
  -writebuf=1024    write buffer size in bytes
```

TODO
----

 - support MCAST
 - support BCAST

