# swarmy-mqtt
Swiss Army Knife MQTT Browser

A web-based MQTT v5 client tool well suited for tracking clusters.

## How to
$ go build -o ./build/

$ cd build

$ ./swarmy-mqtt -h

$ ./swarmy-mqtt -state ./conns.json -db ./msgs.sqlite

Append the -port PORTNUM argument if you want to use a different port than 6080.

Browse to http://localhost:6080 or http://YOURIP:6080 and connect to 1 or more brokers at the same time.