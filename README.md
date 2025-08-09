# distributed-xmlrpc-query-service

This Python program implements a simple distributed query service using XML-RPC with three components:

Worker servers load and serve data from JSON files, providing query methods to get records by name, location, or year.

A master server receives client requests and forwards them to the least-busy available worker, tracking request counts and monitoring worker health with periodic pings.

A client script connects to the master server to perform queries, receiving and displaying results.

The master balances load across workers and handles worker failures by routing requests only to responsive workers, ensuring fault-tolerant, distributed data querying.
