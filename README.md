# Scala support for Apache Kafka's Java client library 0.9.0.x

This project comprises a few helper modules for operating the [Kafka Java Client Driver](https://kafka.apache.org/090/javadoc/index.html) in a Scala codebase.

## Status
These modules are production ready, actively maintained and are used in a large scale production system.

## Scala Kafka Client

A minimal Scala wrapper over the official Apache Kafka Java client library, providing some helpers for convenient configuration the client and usage from Scala.

### Documentation
[Wiki](https://github.com/cakesolutions/scala-kafka-client/wiki/Scala-Kafka-Client)

### Version Compatibility

 scala-kafka-client | Kafka Java Driver
 ------------------ | -----------------
 0.6.x | 0.9.0.1
 0.5.x | 0.9.0.1
 0.4  | 0.9.0.0

### Resolve

    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.6.0"

## Scala Kafka Client - Akka Integration

This module provides a configurable asynchronous and non-blocking Kafka Consumer and Producer Actor implementations to support high performance, parallel custom stream
processing with message delivery guarantees.

### Documentation
[Wiki](https://github.com/cakesolutions/scala-kafka-client/wiki/Akka-Kafka-Client)

### Resolve

    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-akka" % "0.6.0"

## TestKit
 
The TesktKit module provides some tools to support integration testing for client service code that
depends on a running Kafka Server.  Helps the setup of an in-process Kafka and Zookeeper server. 

### Documentation
[Wiki](https://github.com/cakesolutions/scala-kafka-client/wiki/Testkit)

### Resolve

    //For kafka integration test support:
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-tesktkit" % "0.6.0" % "test"

## Artefact Resolution
To include, add the following resolver to the build.sbt

    resolvers += Resolver.bintrayRepo("simonsouter", "maven")

## License
    
 Copyright 2016, Cake Solutions.
    
 Licensed under the MIT License
