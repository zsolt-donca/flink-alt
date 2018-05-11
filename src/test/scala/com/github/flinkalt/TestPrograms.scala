package com.github.flinkalt

import cats.data.State
import cats.instances.int._
import cats.instances.list._
import cats.kernel.Semigroup
import com.github.flinkalt.time.Duration
import org.apache.flink.api.scala._

case class Count[T](value: T, count: Int)
object Count {
  implicit def countSemigroup[T]: Semigroup[Count[T]] = new Semigroup[Count[T]] {
    override def combine(x: Count[T], y: Count[T]): Count[T] = Count(y.value, x.count + y.count)
  }
}

sealed trait Size
case object Small extends Size
case object Large extends Size

object TestPrograms {

  // import the method syntax for the type classes
  import DStream.ops._
  import Stateful.ops._
  import Windowed.ops._

  // the programs themselves

  def totalWordCount[DS[_] : DStream : Stateful](lines: DS[String]): DS[Count[String]] = {
    lines.flatMap(splitToWords).mapWithState(zipWithCount)
  }

  def slidingWordCount[DS[_] : DStream : Windowed](lines: DS[String]): DS[Count[String]] = {
    lines
      .flatMap(splitToWords)
      .map(s => Count(s, 1))
      .windowReduce(SlidingWindow(Duration(4), Duration(2)), _.value)
  }

  def slidingSumsBySize[DS[_] : DStream : Windowed](nums: DS[Int]): DS[(Size, Window, Int)] = {
    nums.windowReduceMapped(SlidingWindow(Duration(10), Duration(5)), i => if (i < 10) Small else Large)((size: Size, win: Window, a: Int) => (size, win, a))
  }

  def totalSlidingSums[DS[_] : DStream : Windowed](nums: DS[Int]): DS[List[Int]] = {
    nums
      .map(i => List(i))
      .windowReduce(SlidingWindow(Duration(10), Duration(2)), _ => ())
  }

  // helpers below

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
}
