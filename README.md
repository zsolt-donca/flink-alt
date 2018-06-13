
# Flink-Alt

## Overview

*Flink-Alt* is an alternative minimalistic typeclass-based API for Flink, abstracting over the type of the data stream, allowing for alternative implementations. Besides the implementation for Flink `DataStream`, we also provide an additional in-memory implementation, useful for testing purposes where you don't want to waste precious seconds for Flink to fire up and tear down an in-memory cluster.

*Flink-Alt* currently targets only streaming jobs (the `DataStream` API) using *event time*. The supported operations include ordinary operations such as `map`, `flatMap`, and `filter`, and also includes stateful operations such as `mapWithState` and windowed operations such as `reduce`.

## Introduction

The library uses the *tagless final* design approach (see [Exploring Tagless Final](https://blog.scalac.io/exploring-tagless-final.html) [Alternatives to GADTs in Scala](https://pchiusano.github.io/2014-05-20/scala-gadts.html) or [Optimizing Tagless Final](https://typelevel.org/blog/2017/12/27/optimizing-final-tagless.html), or one of the many great talks on YouTube).

A sample application looks like this:
```scala
import cats.data.State
import com.github.flinkalt.{DStream, Stateful, StateTrans}
import DStream.ops._
import Stateful.ops._

case class Count[T](value: T, count: Int)

def totalWordCount[DS[_] : DStream : Stateful](lines: DS[String]): DS[Count[String]] = {
  lines
    .flatMap(splitToWords)
    .mapWithState(zipWithCount)
}

def splitToWords(line: String): Seq[String] = {
  line.toLowerCase().split("\\W+").filter(_.nonEmpty)
}

def zipWithCount[T]: StateTrans[T, Int, T, Count[T]] = {
  StateTrans(
    identity,
    t => State(maybeCount => {
      val count = maybeCount.getOrElse(0) + 1
      (Some(count), Count(t, count))
    }))
}
```
 
## Type Information

*Flink-Alt* also offers an alternative to [Flink's macro-based type information derivation](https://ci.apache.org/projects/flink/flink-docs-release-1.5/dev/types_serialization.html#type-information-in-the-scala-api). The type information derivation is based on [shapeless](https://github.com/milessabin/shapeless), and is heavily inspired from [flink-shapeless](https://github.com/joroKr21/flink-shapeless). Its main features include:
 - everything is assumed immutable
 - automatic derivation of type info:
    - primitives: `Boolean`, `Byte`, `Short`, `Char`, `Int`, `Long`, `Float`, `Double`, `Unit` 
    - common types: `String`, `Option`, `Either`, `cats.data.Validated`)
    - byte arrays
    - some collections: `List`, `Vector` and `Map` (not for all `Traversable` because scala's `CanBuild` instances are not serializable)
    - injections, that is, representing some types as another that has `TypeInfo`
    - ADTs, using *shapeless*, that is, any case class or sealed trait for which *shapeless* can derive `Generic`  
 - deduplication of data:
    - when an object graph is serialized with equal values in it (like equal strings, collections, or just about any value), then these values are represented in a single time, saving space and serialization time   
    - when data is deserialized, the duplicate values are not allocated multiple times in memory, thus reducing the memory requirements
    - deduplication is based on the objects' `equals` and `hashCode` methods, so you should implement them efficiently       
 

### Example:

```scala
import org.apache.flink.streaming.api.datastream.DataStream
import com.github.flinkalt.typeinfo.auto._

case class Count[T](value: T, count: Int)

val s1: DataStream[String] = ???
val s2: DataStream[Count[String]] = s1.map(s => Count(s, 1)) // DataStream.map required an implicit TypeInformation for the output, Count[String] in this case

```
