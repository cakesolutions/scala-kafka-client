Scala wrapper for Apache Kafka's Java client library 0.9
========================================================

This project comprises a few helper modules for operating the Kafka Java Client driver. [https://kafka.apache.org/090/javadoc/index.html] in a Scala codebase.

## Status
This project is currently a work in progress and not yet ready for proper use!

## Client
The scala-kafka-client is a thin wrapper around the Java client API, providing some helpers for configuring the client.

### Producer

### Consumer

## Akka
The Akka module provides a Consumer Actor that can be convenient when developing applications with Akka.  The Akka consumer has buffering capabilities to increase throughput as well as some helpers to provide easy configuration.

## TestKit
The scala-kafka-client-tesktkit supports integration testing of Kafka client code by providing helpers that can start an in-process Kafka and Zookeeper server. 

### Latest release
To include, add the following to the build.sbt

    resolvers += Resolver.bintrayRepo("simonsouter", "maven")
    
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.5.0"
    
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
    
 ## License
    
 Copyright 2016, Cake Solutions.
    
 Licensed under the MIT License
