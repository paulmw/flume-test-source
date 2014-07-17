Flume Test Source!
==================

This Flume source/sink pair is useful for testing the correctness of network transports such as Avro and similar.

Test Strategy
-------------

Use a pseudorandom sequence (java.util.Random) with a fixed seed to produce random Flume events. The events have a random number of headers (with random keys and values) and a random-length body containing yet more random data.

Usage
-----

The source has a delay parameter (specified in milliseconds) and a customisable random seed:

	a1.sources.r1.type = com.cloudera.flume.TestSource
	a1.sources.r1.seed = 1
	a1.sources.r1.delay = 200

The sink has a customisable random seed:

	a1.sinks.k1.type = com.cloudera.flume.TestSink
	a1.sinks.k1.seed = 1
 
