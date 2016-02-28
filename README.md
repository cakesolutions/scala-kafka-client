# Scala wrapper for Apache Kafka's Java client library 0.9.x.x

This project comprises a few helper modules for operating the [Kafka Java Client Driver](https://kafka.apache.org/090/javadoc/index.html) in a Scala codebase.

## Status
This project is currently a work-in-progress and not yet ready for proper use!

## Modules

The following modules are provided
 - **scaka-kafka-client.** A thin wrapper around the Java client API, providing some helpers for configuring the client.
 - **scala-kafka-client-akka.** Provides a Consumer Actor that can be convenient when developing applications with Akka.  The Akka consumer has buffering capabilities to increase throughput as well as some helpers to provide easy configuration.
 - **scaka-kafka-client-testkit.** Supports integration testing of Kafka client code by providing helpers that can start an in-process Kafka and Zookeeper server.

### Latest release
To include, add the following to the build.sbt

    resolvers += Resolver.bintrayRepo("simonsouter", "maven")

    For basic kafka client support:
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.5.0"

    For kafka integration test support:
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-tesktkit" % "0.5.0" % "test"

    For akka support:
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-akka" % "0.5.0"

## Version Compatibility

 scala-kafka-client | Kafka Java Driver
 ------------------ | -----------------
 0.5 | 0.9.0.1
 0.4  | 0.9.0.0

## scaka-kafka-client
The scala-kafka-client is a thin wrapper around the Java client API, providing some helpers for configuring the client.

### Producer

### Consumer
The [KafkaConsumer](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) is typically setup by creating a Java Properties with required consumer configuration.
The Scala wrapper provides some convenience functions for creating the consumer configuration either directly in code, via Typesafe config or a combination of both.

#### Direct Configuration
The cakesolutions.kafka.KafkaConsumer.Conf case class models the configuration required for the KafkaConsumer and can be constructed and passed to the cakesolutions.kafka.KafkaConsumer factory method to produce a configured [KafkaConsumer](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)
Key and Value Deserialisers are also required as documented in the Kafka client docs.

```
val consumer = KafkaConsumer(KafkaConsumer.Conf(new StringDeserializer(), new StringDeserializer(), bootstrapServers = "localhost:8082"))
```

The Conf class provides additional properties with defaults:

```
Conf(new StringDeserializer(), new StringDeserializer()
    bootstrapServers = "localhost:9092",
    groupId: = "group",
    enableAutoCommit = true,
    autoCommitInterval = 1000,
    sessionTimeoutMs = 30000
```

#### Typesafe Configuration
The configuration for the KafkaConsumer can also specified in a [Typesafe Config](https://github.com/typesafehub/config) file:  
Pass a Config to cakesolutions.kafka.KafkaConsumer.Conf: 

```
application.conf:

{
      bootstrap.servers = "localhost:8082"
}

val conf = ConfigFactory.load

val consumer = KafkaConsumer(KafkaConsumer.Conf(new StringDeserializer(), new StringDeserializer(), conf))
```

#### Additional Config Options
A combination of static properties and Typesafe config properties is also an option.  The Typesafe config properties will override predefined properties:

```
val conf = ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:8082",
           | group.id = "group1"
        """.stripMargin)

val conf = Conf(new StringDeserializer(), new StringDeserializer(), enableAutoCommit = false).withConf(conf)
val consumer = KafkaConsumer(conf)
```

## scala-kafka-client-akka
The Akka module provides a Consumer Actor that can be convenient when developing applications with Akka.

### Asynchronous and Non Blocking Kafka Consumer
This module provides a configurable KafkaConsumerActor which utilises Akka to provide an asynchronous and non-blocking Kafka consumer,
which is often desirable when building highly scalable and reactive applications with a Scala stack.

The basic KafkaConsumer provided by the Kafka Java client is not thread safe and must be driven by a client poll thread,
typically from a blocking style poll loop.  While this type of approach may be adequate for many applications, there are
some known drawbacks:
 
1. Threading code must be implemented to facilitate the poll loop.
2. One thread is required per consumer.
3. Network IO and message processing occurs on the same thread, increasing round-trip latency.

The KafkaConsumerActor utilises Akka's message dispatch architecture to implement an asynchronous consumer with a poll loop that pull and buffer records from
Kafka and dispatches to the client asynchronously with a separate thread (analogous to the [Reactor Pattern](https://en.wikipedia.org/wiki/Reactor_pattern)).
 
- TODO Confirmation pattern (at least once)  - commit modes (redelivery) - caching

### Configuration


### Message Exchange Patterns


## scaka-kafka-client-testkit
The scala-kafka-client-tesktkit supports integration testing of Kafka client code by providing helpers that can start an in-process Kafka and Zookeeper server. 

## License
    
 Copyright 2016, Cake Solutions.
    
 Licensed under the MIT License
