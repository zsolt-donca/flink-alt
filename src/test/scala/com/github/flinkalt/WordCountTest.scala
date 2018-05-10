package com.github.flinkalt

import cats.data.State
import cats.kernel.Semigroup
import com.github.flinkalt.memory.{Data, MemoryDStream}
import com.github.flinkalt.time.{Duration, Instant}
import org.scalatest.FunSuite

class WordCountTest extends FunSuite {

  import DStream.ops._

  test("Total Word Count") {
    def wordCount[DS[_] : DStream](lines: DS[String]): DS[Count[String]] = {
      lines
        .flatMap(splitToWords)
        .mapWithState(zipWithCount)
    }

    val stream = MemoryDStream(Vector(
      data(timeAt(0), "x"),
      data(timeAt(1), "y z"),
      data(timeAt(2), ""),
      data(timeAt(5), "z q y y")
    ))

    val outStream: MemoryDStream[Count[String]] = wordCount(stream)

    val actualValues = outStream.vector
    assert(actualValues == Vector(
      data(timeAt(0), Count("x", 1)),
      data(timeAt(1), Count("y", 1)),
      data(timeAt(1), Count("z", 1)),
      data(timeAt(5), Count("z", 2)),
      data(timeAt(5), Count("q", 1)),
      data(timeAt(5), Count("y", 2)),
      data(timeAt(5), Count("y", 3))
    ))
  }

  test("Sliding Word Count") {
    implicit def countSemigroup[T]: Semigroup[Count[T]] = new Semigroup[Count[T]] {
      override def combine(x: Count[T], y: Count[T]): Count[T] = Count(y.value, x.count + y.count)
    }

    def wordCount[DS[_] : DStream](lines: DS[String]): DS[Count[String]] = {
      lines
        .flatMap(splitToWords)
        .map(s => Count(s, 1))
        .windowReduce(SlidingWindow(Duration(4), Duration(2)), WindowReduce(_.value))
    }

    val stream = MemoryDStream(Vector(
      data(timeAt(0), "x"),
      data(timeAt(2), "y z"),
      data(timeAt(3), "y"),
      data(timeAt(6), "z q y y")
    ))

    val outStream: MemoryDStream[Count[String]] = wordCount(stream)

    val actualValues = outStream.vector
    assert(actualValues == Vector(
      data(timeAt(2), Count("x", 1)),

      data(timeAt(4), Count("y", 2)),
      data(timeAt(4), Count("x", 1)),
      data(timeAt(4), Count("z", 1)),

      data(timeAt(6), Count("z", 1)),
      data(timeAt(6), Count("y", 2)),

      data(timeAt(8), Count("q", 1)),
      data(timeAt(8), Count("z", 1)),
      data(timeAt(8), Count("y", 2)),

      data(timeAt(10), Count("q", 1)),
      data(timeAt(10), Count("z", 1)),
      data(timeAt(10), Count("y", 2))
    ))
  }

  test("Sliding number ladder") {
    import cats.instances.int._

    sealed trait Size
    case object Small extends Size
    case object Large extends Size

    def slidingSumsByDecimal[DS[_] : DStream](nums: DS[Int]): DS[(Size, Window, Int)] = {
      nums.windowReduce(SlidingWindow(Duration(10), Duration(5)), WindowReduce(i => if (i < 10) Small else Large, (size: Size, win: Window, a: Int) => (size, win, a)))
    }

    val stream = MemoryDStream(Vector(
      data(timeAt(0), 1),
      data(timeAt(0), 2),
      data(timeAt(0), 10),
      data(timeAt(0), 12),

      data(timeAt(3), 3),
      data(timeAt(3), 3),

      data(timeAt(7), 4),
      data(timeAt(7), 13),

      data(timeAt(11), 11),
      data(timeAt(12), 7)
    ))

    val outStream: MemoryDStream[(Size, Window, Int)] = slidingSumsByDecimal(stream)
    val actualValues = outStream.vector
    assert(actualValues == Vector(
      data(timeAt(5), (Large, Window(start = timeAt(-5), end = timeAt(5)), 10 + 12)),
      data(timeAt(5), (Small, Window(start = timeAt(-5), end = timeAt(5)), 1 + 2 + 3 + 3)),

      data(timeAt(10), (Large, Window(start = timeAt(0), end = timeAt(10)), 10 + 12 + 13)),
      data(timeAt(10), (Small, Window(start = timeAt(0), end = timeAt(10)), 1 + 2 + 3 + 3 + 4)),

      data(timeAt(15), (Large, Window(start = timeAt(5), end = timeAt(15)), 13 + 11)),
      data(timeAt(15), (Small, Window(start = timeAt(5), end = timeAt(15)), 4 + 7)),

      data(timeAt(20), (Small, Window(start = timeAt(10), end = timeAt(20)), 7)),
      data(timeAt(20), (Large, Window(start = timeAt(10), end = timeAt(20)), 11))
    ))
  }

  private def data[T](time: Instant, value: T): Data[T] = Data(time, time, value)

  private def timeAt(i: Int): Instant = Instant(1000L + i)

  def splitToWords(line: String): Seq[String] = {
    line.toLowerCase().split("\\W+").filter(_.nonEmpty)
  }

  case class Count[T](value: T, count: Int)

  def zipWithCount[T]: StateTrans[T, Int, T, Count[T]] = {
    StateTrans(
      identity,
      t => State(maybeCount => {
        val count = maybeCount.getOrElse(0) + 1
        (Some(count), Count(t, count))
      }))
  }
}
