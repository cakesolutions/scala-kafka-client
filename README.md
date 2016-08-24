# Scala support for Apache Kafka's Java client library 0.9.0.x and 0.10.0.x

[![Join the chat at https://gitter.im/cakesolutions/scala-kafka-client](https://badges.gitter.im/cakesolutions/scala-kafka-client.svg)](https://gitter.im/cakesolutions/scala-kafka-client?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build status](https://travis-ci.org/cakesolutions/scala-kafka-client.svg?branch=master)](https://travis-ci.org/cakesolutions/scala-kafka-client)
[![Dependencies](https://app.updateimpact.com/badge/748875216658239488/root.svg?config=compile)](https://app.updateimpact.com/latest/748875216658239488/root)

This project comprises a few helper modules for operating the [Kafka Java Client Driver](https://kafka.apache.org/0100/javadoc/index.html) in a Scala codebase.

* [Scala Kafka Client](#scala-kafka-client)
* [Scala Kafka Client - Akka Integration](#scala-kafka-client---akka-integration)
* [TestKit](#testkit)

<img src="https://raw.githubusercontent.com/wiki/cakesolutions/scala-kafka-client/images/logo.png" align="sck" height="250" width="300">

## Status

These modules are production ready, actively maintained and are used in a large scale production system.

## Artifact Resolution

To resolve any of the modules, add the following resolver to the build.sbt:

    resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

## Components

### Scala Kafka Client

A thin Scala wrapper over the official [Apache Kafka Java Driver](http://kafka.apache.org/documentation.html#api).
This module is useful for integrating with Kafka for message consumption/delivery,
but provides some helpers for convenient configuration of the driver and usage from Scala.
Minimal 3rd party dependencies are added in addition to the Kafka client.

For configuration and usage, see the Wiki:
[Scala Kafka Client Guide](https://github.com/cakesolutions/scala-kafka-client/wiki/Scala-Kafka-Client)

SBT library dependency:

```scala
libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.10.0.0"
```

### Akka Integration

This module provides a configurable asynchronous and non-blocking Kafka Consumer and Producer Actor implementations to support high performance, parallel custom stream processing in an Akka application.
These components are specifically intended for use cases where high performance and scalable message processing is required with specific concern for message delivery guarantees and resilience.

For configuration and usage, see the Wiki:
[Akka Integration](https://github.com/cakesolutions/scala-kafka-client/wiki/Akka-Integration)

SBT library dependency:

```scala
libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-akka" % "0.10.0.0"
```

### TestKit

The TestKit module provides some tools to support integration testing of client service code that depends on a running Kafka Server.
Helps the setup of an in-process Kafka and Zookeeper server.

For usage, see the Wiki:
[TestKit User Guide](https://github.com/cakesolutions/scala-kafka-client/wiki/Testkit)

SBT library dependency:

```scala
libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-testkit" % "0.10.0.0" % "test"
```

## Version Compatibility

Starting after version `0.8.0`, the versioning for Scala Kafka client will be tracking Kafka's versioning scheme.
Binary compatibility in the new versioning system works as follows:

* The first and the second digit in the version indicate compatibility with the Kafka driver.
  For example, `0.9.0.0` is compatible with Kafka 0.9 and `0.10.0.0` is compatible with Kafka 0.10.
* The third digit in the version indicates an incompatible change between Scala Kafka client versions.
  For example, `0.9.0.1` is not binary compatible with `0.9.1.0`.
* The fourth digit in the version indicates a compatible change between Scala Kafka client versions.
  For example, `0.9.0.0` is compatible with `0.9.0.1`.

Both the `0.9.*` and `0.10.*` versions are maintained concurrently.

Here is the full table of binary compatibilities between Scala Kafka client and the Kafka Java driver:

 Scala Kafka client    | Kafka Java Driver
 --------------------- | -----------------
 0.10.0.0              | 0.10.0.x
 0.9.0.0               | 0.9.0.x
 0.8.0                 | 0.10.0.0
 0.7.0                 | 0.9.0.1

## Change log

### 0.9.0.0,0.10.0.0 - 08/2016

* Subscribe model changed, now supports more explicit subscription types
* Handling of partition rebalances improved
* ConsumerActor failure and restart mechanics improved
* Versioning scheme changed
* Testkit improvements
* ConsumerActor wrapper API provided
* Tested against Kafka 0.10.0.1

### 0.8.0 - 06/2016

* Supports Kafka Client 0.10.0.0
* Add max.poll.records config option to consumer

### 0.7.0 - 05/2016

* Supports Kafka Client 0.9.0.1

## Acknowledgements

<img src="https://www.yourkit.com/images/yklogo.png" align="right" />
YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of [YourKit Java Profiler](https://www.yourkit.com/java/profiler/index.jsp)
and [YourKit .NET Profiler](https://www.yourkit.com/.net/profiler/index.jsp),
innovative and intelligent tools for profiling Java and .NET applications.

## License

Copyright 2016, Cake Solutions.

Licensed under the MIT License
