# Scala extensions for Apache Kafka's Java client library 0.9.0.x

This project comprises a few helper modules for operating the [Kafka Java Client Driver](https://kafka.apache.org/090/javadoc/index.html) in a Scala codebase.

## Status
This project is currently a work-in-progress and not yet ready for proper use!

## Modules

The following modules are provided
 - **scaka-kafka-client.** A thin wrapper around the Java client API, providing some helpers for convenient configuration the client.
 - **scala-kafka-client-akka.** Provides a Consumer Actor that can be convenient when developing applications with Akka.  The Akka consumer has buffering capabilities to increase throughput as well as some helpers to provide easy configuration.
 - **scaka-kafka-client-testkit.** Supports integration testing of Kafka client code by providing helpers that can start an in-process Kafka and Zookeeper server.

### Latest release
To include, add the following resolver to the build.sbt

    resolvers += Resolver.bintrayRepo("simonsouter", "maven")

## Version Compatibility

 scala-kafka-client | Kafka Java Driver
 ------------------ | -----------------
 0.5 | 0.9.0.1 | Not Yet Released!
 0.4  | 0.9.0.0

## Scala Kafka Client
The scala-kafka-client is a thin wrapper around the Java client API, providing some helpers for configuring the client.

### Resolve

    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.5.0"
    
### Producer

TODO

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

## Scala Kafka Client - Async
The scala-kafka-client-akka module provides an asynchronous and non-blocking Kafka Consumer built using Akka, that 
can be useful when developing [Reactive](http://www.reactivemanifesto.org/) applications or when high throughput and scalability are required.

### Resolve

    // Latest release:
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-akka" % "0.5.0"

### Motivation
This module provides a configurable KafkaConsumerActor which utilises Akka to provide an asynchronous and non-blocking Kafka consumer,
which is often desirable when building highly scalable and reactive applications with a Scala stack.

The basic KafkaConsumer provided by the Kafka Java client is not thread safe and must be driven by a client poll thread,
typically from a blocking style poll loop.  While this type of approach may be adequate for many applications, there are
some known drawbacks:
 
1. Threading code must be implemented to facilitate the poll loop.
2. One thread is required per consumer.
3. Network IO and message processing occurs on the same thread, increasing round-trip latency.

The KafkaConsumerActor utilises Akka's message dispatch architecture to implement an asynchronous consumer with an
[Akka Scheduler](http://doc.akka.io/docs/akka/snapshot/scala/scheduler.html) driven poll loop that requests and buffers records from
Kafka and dispatches to the client asynchronously on a separate thread (analogous to the [Reactor Pattern](https://en.wikipedia.org/wiki/Reactor_pattern)).
 
- TODO Confirmation pattern (at least once)  - commit modes (redelivery) - caching

### Configuration
To create a KafkaConsumerActor the dependencies in the KafkaConsumerActor.props() function need to be satisfied.  This can be 
done with a Key and Value deserializer with all other consumer properties supplied in a Typesafe configuration.

```
{
    //KafkaConsumer config
    bootstrap.servers = "localhost:9092",
    group.id = "test"
    enable.auto.commit = false
    auto.offset.reset = "earliest"
    consumer.topics = ["topic1"]
    //KafkaConsumerActor config
    schedule.interval = 3000 milliseconds
    unconfirmed.timeout = 3000 milliseconds
    buffer.size = 8
}
       
```

An alternative approach
is to provide KafkaConsumer.Conf and KafkaConsumerActor.Conf configuration case classes which can be created in the following ways:

#### KafkaConsumer.Conf

- TODO

#### KafkaConsumerActor.Conf

- TODO

### Message Exchange Patterns
Once the KafkaConsumerActor is created with the required configuration, communication between the client and the consumer actor
is via Actor messages.

#### Subscribe

```
case class Subscribe(offsets: Option[Offsets] = None)

consumer ! Subscribe()

```

Initiates a subscription based on initial configuration.  Initial offets may be provided to seek from a known point in each Topic+Partition
that consumer is subscribed to.  This allows the client to self manage commit offets as described [here](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)
in section "Manual Offset Control".
 
If no offsets are provided (i.e. Subscribe()), the consumer will seek (for each topic/partition) to the last committed offset as
recorded by Kafka itself, and also based on the "auto.offset.reset" client property.

Once the client has sent the consumer a Subscribe message, it can assume that the subscription will be made and any 
messages destined for it will be received by the provided actor callback.  Any failure to connect to Kafka or other exception
will be propagated to the supervisor actor.

### Records[K, V]

```
case class Records(offsets: Offsets, records: ConsumerRecords[K, V])
```

The payload delivered to the client contains the offsets for the records sent and the ConsumerRecords,
which contains a sequence of Records for each Topic Partition.  The ConsumerRecords can be iterated and read as described
in the Kafka Client docs.  The Offsets can be used when confirming the message to commit the offsets to Kafka. 

### Confirm

```
case class Confirm(offsets: Option[Offsets] = None)

consumer ! Confirm()
```

For each Records received by the client, a corresponding Confirm() message should be sent back to the consumer
to acknowledge the message.  If the message is not confirmed within ("unconfirmed.timeout") it is redelivered (by default).

If offsets are provided, they are commited to Kafka.  If no offsets are provided, the message is removed from the Consumer
Actor's buffer, but is not commited to Kafka.

### Unsubscribe

```
case object Unsubscribe

consumer ! Unsubscribe
```

THe Actor clears its state and disconnects from Kakfa.

## TestKit 
The scala-kafka-client-tesktkit module provides some tools to support integration testing for client service code that
depends on a running Kafka Server.  Helps the setup of an in-process Kafka and Zookeeper server. 

### Resolve

    //For kafka integration test support:
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-tesktkit" % "0.5.0" % "test"

## License
    
 Copyright 2016, Cake Solutions.
    
 Licensed under the MIT License
