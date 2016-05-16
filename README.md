# Scala support for Apache Kafka's Java client library 0.9.0.x

This project comprises a few helper modules for operating the [Kafka Java Client Driver](https://kafka.apache.org/090/javadoc/index.html) in a Scala codebase.

* [Scala Kafka Client](#scala-kafka-client)
* [Scala Kafka Client - Akka Integration](#scala-kafka-client---akka-integration)
* [TestKit](#testkit)

## Status
These modules are production ready, actively maintained and are used in a large scale production system.

## Artifact Resolution
To resolve any of the modules, add the following resolver to the build.sbt:

    resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

## Scala Kafka Client

A thin Scala wrapper over the official [Apache Kafka Java Driver](http://kafka.apache.org/documentation.html#api).
This module is useful for integrating with Kafka for message consumption/delivery, but provides some helpers for convenient 
configuration of the driver and usage from Scala.  Minimal 3rd party dependencies are added in addition to the Kafka client.

### Documentation
For configuration and usage, see the Wiki: [Scala Kafka Client Guide](https://github.com/cakesolutions/scala-kafka-client/wiki/Scala-Kafka-Client)

### Version Compatibility

 scala-kafka-client | Kafka Java Driver
 ------------------ | -----------------
 0.7.x | 0.9.0.1
 0.6.x | 0.9.0.1
 0.5.x | 0.9.0.1
 0.4  | 0.9.0.0

### Resolve

    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.7.0"

## Scala Kafka Client - Akka Integration

This module provides a configurable asynchronous and non-blocking Kafka Consumer and Producer Actor implementations to support high performance, parallel custom stream
processing in an Akka application.  These components are specifically intended for use cases where high performance and scalable message processing is required with specific
concern for message delivery guarantees and resilience.

### Documentation
[Kafka Client - Akka Integration](https://github.com/cakesolutions/scala-kafka-client/wiki/Akka-Kafka-Client)

### Resolve

    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-akka" % "0.7.0"

## TestKit
 
The TesktKit module provides some tools to support integration testing of client service code that
depends on a running Kafka Server.  Helps the setup of an in-process Kafka and Zookeeper server. 

### Documentation
[TestKit User Guide](https://github.com/cakesolutions/scala-kafka-client/wiki/Testkit)

### Resolve

    //For kafka integration test support:
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-tesktkit" % "0.7.0" % "test"

## License
    
 Copyright 2016, Cake Solutions.
    
 Licensed under the MIT License
