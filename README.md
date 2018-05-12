
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
 