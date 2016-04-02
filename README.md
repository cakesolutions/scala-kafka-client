# Scala support for Apache Kafka's Java client library 0.9.0.x

This project comprises a few helper modules for operating the [Kafka Java Client Driver](https://kafka.apache.org/090/javadoc/index.html) in a Scala codebase.

## Status
These modules are actively maintained and are used in a large scale production system.

## Modules

The following modules are provided
 - **scaka-kafka-client.** A minimal Scala wrapper around the Java client API, providing some helpers for convenient configuration the client and usage from Scala.
 - **scala-kafka-client-akka.** Provides an Asynchronous and non-blocking Kafka Consumer that can be convenient when developing applications with Akka.
 The KafkaConsumerActor has buffering capabilities to increase throughput as well as some helpers to provide easy configuration.
 - **scaka-kafka-client-testkit.** Supports integration testing of Kafka client code by providing helpers that can start an in-process Kafka and Zookeeper server.

### Latest release
To include, add the following resolver to the build.sbt

    resolvers += Resolver.bintrayRepo("simonsouter", "maven")

## Version Compatibility

 scala-kafka-client | Kafka Java Driver
 ------------------ | -----------------
 0.5.x | 0.9.0.1
 0.4  | 0.9.0.0

## Scala Kafka Client
The scala-kafka-client is a minimal Scala wrapper around the Java client API, providing some helpers for configuring the client.

### Resolve

    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.5.1"
    
### Producer

TODO

### Consumer
The [KafkaConsumer](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) is typically setup by creating a Java Properties with required consumer configuration.
The Scala wrapper provides some convenience functions for creating the consumer configuration either directly in code, via Typesafe config or a combination of both.

#### Direct Configuration
The `cakesolutions.kafka.KafkaConsumer.Conf` case class models the configuration required for the KafkaConsumer and can be constructed and passed to the
`cakesolutions.kafka.KafkaConsumer` factory method to produce a configured [KafkaConsumer](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)
Key and Value Deserialisers are also required as documented in the Kafka client docs.

```
import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.KafkaConsumer.Conf

// Create a org.apache.kafka.clients.consumer.KafkaConsumer
val consumer = KafkaConsumer(
    Conf(new StringDeserializer(), new StringDeserializer(), bootstrapServers = "localhost:8082")
)
```

The Conf configuration class provides additional properties with defaults:

```
import cakesolutions.kafka.KafkaConsumer.Conf

Conf(new StringDeserializer(), new StringDeserializer()
  bootstrapServers = "localhost:9092",
  groupId: = "group",
  enableAutoCommit = true,
  autoCommitInterval = 1000,
  sessionTimeoutMs = 30000,
  maxPartitionFetchBytes: String = "262144",
  autoOffsetReset: OffsetResetStrategy = OffsetResetStrategy.LATEST
)
```

#### Typesafe Configuration
The configuration for the KafkaConsumer can also specified in a [Typesafe Config](https://github.com/typesafehub/config) file:  
Pass a Typesafe Config to cakesolutions.kafka.KafkaConsumer.Conf:

```
application.conf:
{
   bootstrap.servers = "localhost:8082"
}

import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.KafkaConsumer.Conf

val conf = ConfigFactory.load

// Create a org.apache.kafka.clients.consumer.KafkaConsumer from properties defined in a Typesafe Config file
val consumer = KafkaConsumer(
  Conf(conf, new StringDeserializer(), new StringDeserializer())
)
```

#### Additional Config Options
A combination of static properties and Typesafe config properties is also an option.  The Typesafe config properties will override Conf parameters:

```
import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.KafkaConsumer.Conf

val conf = ConfigFactory.parseString(
  s"""
    | bootstrap.servers = "localhost:8082",
    | group.id = "group1"
  """.stripMargin)

val consumer = KafkaConsumer(
  Conf(new StringDeserializer(), new StringDeserializer(), enableAutoCommit = false).withConf(conf)
)
```

## Scala Kafka Client Akka
The scala-kafka-client-akka module provides an asynchronous and non-blocking Kafka Consumer built using Akka, that 
can be useful when developing [Reactive](http://www.reactivemanifesto.org/) applications or when high throughput and scalability are required.

### Resolve

    // Latest release:
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-akka" % "0.5.1"

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
To create a KafkaConsumerActor, the dependencies in the KafkaConsumerActor.props() function need to be satisfied.  This can be
done with a Key and Value deserializer with all other consumer properties supplied in a Typesafe configuration.

```
//application.conf
{
    // Standard KafkaConsumer properties:
    bootstrap.servers = "localhost:9092",
    group.id = "test"
    enable.auto.commit = false
    auto.offset.reset = "earliest"
    topics = ["topic1"]

    //KafkaConsumerActor config
    schedule.interval = 3000 milliseconds
    unconfirmed.timeout = 3000 milliseconds
    buffer.size = 8
}

import cakesolutions.kafka.akka.KafkaConsumerActor

val consumer = system.actorOf(
  KafkaConsumerActor.props(conf, new StringDeserializer(), new StringDeserializer(), self)
)
```

#### KafkaConsumer.Conf and KafkaConsumerActor.Conf

An alternative approach is to provide KafkaConsumer.Conf and KafkaConsumerActor.Conf configuration case classes which can be created in the following ways:

```
import scala.concurrent.duration._
import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.akka.KafkaConsumerActor

// Configuration for the KafkaConsumer
val consumerConf = KafkaConsumer.Conf(
    new StringDeserializer,
    new StringDeserializer,
    bootstrapServers = s"localhost:9092",
    groupId = "groupId",
    enableAutoCommit = false)

// Configuration specific to the Async Consumer Actor
val actorConf = KafkaConsumerActor.Conf(List("topic1"), 1.seconds, 3.seconds)

// Create the Actor
val consumer = system.actorOf(
  KafkaConsumerActor.props(consumerConf, actorConf, self)
)

```

### Receiver Actor
In each of the above configuration examples it is assumed the Consumer Actor is created from the context of a parent actor,
which is passed to the consumer via a reference to 'self'.  This is an ActorRef to which consumed messages will be delivered and
should expect to receive `cakesolutions.kafka.akka.KafkaConsumerActor.Records[K,V]` containing a batch of
[ConsumerRecords](https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/ConsumerRecords.html) consumed from Kafka.

```
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Records}

class ReceiverActor extends Actor {

  override def receive:Receive = {
    case r:Records[_, _] =>
      
      //Type safe cast of records to correct serialisation type
      r.cast[String, String] match {
        case Some(records) =>
          processRecords(records.records)
          sender() ! Confirm(r.offsets)
        case None => log.warning("Received wrong Kafka records type!")
      }
  }
```

### Message Exchange Patterns
Once the KafkaConsumerActor is created with the required configuration, communication between the client/receiver actor and the consumer actor
is naturally via Actor messages.

#### Subscribe

```
case class Subscribe(offsets: Option[Offsets] = None)

consumer ! Subscribe()

```

Sent to the Consumer Actor to initiate a subscription based on initial configuration.  Initial offets may be provided to seek from a known point in each Topic+Partition
that consumer is subscribed to.  This allows the client to self manage commit offsets as described [here](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)
in section "Manual Offset Control".
 
If no offsets are provided (i.e. Subscribe()), the consumer will seek (for each topic/partition) to the last committed offset as
recorded by Kafka itself, and also based on the "auto.offset.reset" client property.

Once the client has sent the consumer a Subscribe message, it can assume that the subscription will be made and any 
messages consumer will be delivered to the provided ActorRef callback.  Any failure to connect to Kafka or other exception
will be propagated via the standard [Actor Supervision](http://doc.akka.io/docs/akka/2.4.2/general/supervision.html) mechanism.

### Records[K, V]

```
case class Records(offsets: Offsets, records: ConsumerRecords[K, V])
```

The payload delivered to the client contains the offsets for the records sent and the `ConsumerRecords`,
which contains a sequence of Records for each Topic Partition.  The `ConsumerRecords` can be iterated and read as described
in the Kafka Client docs.  The Offsets can be used when confirming the message to commit the offsets to Kafka. 

### Confirm

```
case class Confirm(offsets: Offsets, commit: Boolean = false)

consumer ! Confirm(offsets)
```

For each set of records received by the client, a corresponding `Confirm(offsets)` message should be sent back to the consumer
to acknowledge the message.  If the message is not confirmed within ("unconfirmed.timeout") it is redelivered (by default).

If commit is provided as true, they offsets are committed to Kafka.  If commit is false, the records are removed from the Consumer
Actor's buffer, but no commit to Kafka is made.

### Unsubscribe

```
case object Unsubscribe

consumer ! Unsubscribe
```

The Consumer Actor clears its state and disconnects from Kakfa.

## TestKit 
The scala-kafka-client-tesktkit module provides some tools to support integration testing for client service code that
depends on a running Kafka Server.  Helps the setup of an in-process Kafka and Zookeeper server. 

### Resolve

    //For kafka integration test support:
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-tesktkit" % "0.5.1" % "test"

## License
    
 Copyright 2016, Cake Solutions.
    
 Licensed under the MIT License
