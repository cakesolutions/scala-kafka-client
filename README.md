Scala wrapper for Apache Kafka's Java client library 0.9
========================================================

Dependencies
------------

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
    
 | scala-kafka-client | Kafka Java Driver |
 | ------------------ | ----------------- |
 | 0.5                | 0.9.0.1           |
 | 0.4                | 0.9.0.0           |
    
 ## License
    
 Copyright 2016, Cake Solutions.
    
 Licensed under the MIT License
