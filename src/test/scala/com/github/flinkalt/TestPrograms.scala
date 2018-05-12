package com.github.flinkalt

import cats.data.State
import cats.instances.int._
import cats.instances.list._
import cats.kernel.Semigroup
import com.github.flinkalt.memory.Data
import com.github.flinkalt.time._
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

// necessary boilerplate, Scala does not have built-in the concept of polymorphic functions
// also, it turns out there can be many ways a function can be "polymorphic"
// this is neither what shapeless nor what cats calls polymorphic functions
trait DStreamFun[A, B] {
  def apply[DS[_] : DStream : Windowed : Stateful]: DS[A] => DS[B]
}

case class TestCase[A, B]
(
  input: Vector[Data[A]],
  output: Vector[Data[B]],
  program: DStreamFun[A, B]
)

object TestPrograms {

  // import the method syntax for the type classes
  import DStream.ops._
  import Stateful.ops._
  import Windowed.ops._

  // the programs themselves

  // simple transformations, no state nor windowing
  def numberJuggling[DS[_] : DStream](input: DS[Int]): DS[String] = {
    input
      .map(i => i * 5)
      .filter(i => i % 20 != 0)
      .flatMap(i => List(i, i / 2))
      .collect({
        case i if i % 2 == 0 => s"even: $i"
        case i if i % 2 != 0 => s"odd: $i"
      })
  }

  val numberJugglingTestCase = TestCase(
    input = Vector(
      syncedData(at(0 seconds), 1),
      syncedData(at(1 seconds), 3),
      syncedData(at(2 seconds), 4),
      syncedData(at(3 seconds), 2)
    ),
    // watermark lags behind when doing simple transformations as well, without any effect
    output = Vector(
      Data(time = at(0 seconds), watermark = Instant.minValue, "odd: 5"),
      Data(time = at(0 seconds), watermark = Instant.minValue, "even: 2"),
      Data(time = at(1 seconds), watermark = at(0 seconds), "odd: 15"),
      Data(time = at(1 seconds), watermark = at(0 seconds), "odd: 7"),
      Data(time = at(3 seconds), watermark = at(2 seconds), "even: 10"),
      Data(time = at(3 seconds), watermark = at(2 seconds), "odd: 5")
    ),
    program = new DStreamFun[Int, String] {
      override def apply[DS[_] : DStream : Windowed : Stateful]: DS[Int] => DS[String] = numberJuggling
    }
  )


  // word count using a stateful transformation counting each element, each value having an individual state
  def totalWordCount[DS[_] : DStream : Stateful](lines: DS[String]): DS[Count[String]] = {
    lines
      .flatMap(splitToWords)
      .mapWithState(zipWithCount)
  }

  val totalWordCountTestCase = TestCase(
    input = Vector(
      syncedData(at(0 seconds), "x"),
      syncedData(at(1 seconds), "y z"),
      syncedData(at(2 seconds), ""),
      syncedData(at(5 seconds), "z q y y")
    ),
    // watermarks are always lagging behind, coming from the previous value (and thus Long.MinValue for the first)
    output = Vector(
      Data(time = at(0 seconds), watermark = Instant.minValue, Count("x", 1)),
      Data(time = at(1 seconds), watermark = at(0 seconds), Count("y", 1)),
      Data(time = at(1 seconds), watermark = at(0 seconds), Count("z", 1)),
      Data(time = at(5 seconds), watermark = at(2 seconds), Count("z", 2)),
      Data(time = at(5 seconds), watermark = at(2 seconds), Count("q", 1)),
      Data(time = at(5 seconds), watermark = at(2 seconds), Count("y", 2)),
      Data(time = at(5 seconds), watermark = at(2 seconds), Count("y", 3))
    ),
    program = new DStreamFun[String, Count[String]] {
      override def apply[DS[_] : DStream : Windowed : Stateful]: DS[String] => DS[Count[String]] = totalWordCount
    })


  // sliding word count, with a (sliding) window type, each value having an individual state
  def slidingWordCount[DS[_] : DStream : Windowed](windowType: WindowType)(lines: DS[String]): DS[Count[String]] = {
    lines
      .flatMap(splitToWords)
      .map(s => Count(s, 1))
      .windowReduce(windowType, _.value)
  }

  val slidingWordCountTestCase = TestCase(
    input = Vector(
      syncedData(at(0 seconds), "x"),
      syncedData(at(2 seconds), "y z"),
      syncedData(at(3 seconds), "y"),
      syncedData(at(6 seconds), "z q y y")
    ),
    output = Vector(
      Data(time = justBefore(2 seconds), watermark = at(0 seconds), Count("x", 1)),

      Data(time = justBefore(4 seconds), watermark = at(3 seconds), Count("x", 1)),
      Data(time = justBefore(4 seconds), watermark = at(3 seconds), Count("y", 2)),
      Data(time = justBefore(4 seconds), watermark = at(3 seconds), Count("z", 1)),

      Data(time = justBefore(6 seconds), watermark = at(3 seconds), Count("y", 2)),
      Data(time = justBefore(6 seconds), watermark = at(3 seconds), Count("z", 1)),

      Data(time = justBefore(8 seconds), watermark = at(6 seconds), Count("y", 2)),
      Data(time = justBefore(8 seconds), watermark = at(6 seconds), Count("q", 1)),
      Data(time = justBefore(8 seconds), watermark = at(6 seconds), Count("z", 1)),

      Data(time = justBefore(10 seconds), watermark = at(6 seconds), Count("q", 1)),
      Data(time = justBefore(10 seconds), watermark = at(6 seconds), Count("y", 2)),
      Data(time = justBefore(10 seconds), watermark = at(6 seconds), Count("z", 1))
    ),
    program = new DStreamFun[String, Count[String]] {
      override def apply[DS[_] : DStream : Windowed : Stateful]: DS[String] => DS[Count[String]] = {
        slidingWordCount(SlidingWindow(4 seconds, 2 seconds))
      }
    }
  )


  def slidingSumsBySize[DS[_] : DStream : Windowed](windowType: WindowType)(nums: DS[Int]): DS[(Size, Window, Int)] = {
    val key: Int => Size = i => if (i < 10) Small else Large
    nums.windowReduceMapped(windowType, key)((size: Size, win: Window, a: Int) => (size, win, a))
  }


  val slidingSumsBySizeTestCase = TestCase(
    input = Vector(
      syncedData(at(0 seconds), 1),
      syncedData(at(0 seconds), 2),
      syncedData(at(0 seconds), 10),
      syncedData(at(0 seconds), 12),

      syncedData(at(3 seconds), 3),
      syncedData(at(3 seconds), 3),

      syncedData(at(7 seconds), 4),
      syncedData(at(7 seconds), 13),

      syncedData(at(11 seconds), 11),
      syncedData(at(12 seconds), 7)
    ),
    output = Vector(
      Data(time = justBefore(5 seconds), watermark = at(3 seconds), (Small, Window(start = at(-5 seconds), end = at(5 seconds)), 1 + 2 + 3 + 3)),
      Data(time = justBefore(5 seconds), watermark = at(3 seconds), (Large, Window(start = at(-5 seconds), end = at(5 seconds)), 10 + 12)),

      Data(time = justBefore(10 seconds), watermark = at(7 seconds), (Small, Window(start = at(0 seconds), end = at(10 seconds)), 1 + 2 + 3 + 3 + 4)),
      Data(time = justBefore(10 seconds), watermark = at(7 seconds), (Large, Window(start = at(0 seconds), end = at(10 seconds)), 10 + 12 + 13)),

      Data(time = justBefore(15 seconds), watermark = at(12 seconds), (Large, Window(start = at(5 seconds), end = at(15 seconds)), 13 + 11)),
      Data(time = justBefore(15 seconds), watermark = at(12 seconds), (Small, Window(start = at(5 seconds), end = at(15 seconds)), 4 + 7)),

      Data(time = justBefore(20 seconds), watermark = at(12 seconds), (Large, Window(start = at(10 seconds), end = at(20 seconds)), 11)),
      Data(time = justBefore(20 seconds), watermark = at(12 seconds), (Small, Window(start = at(10 seconds), end = at(20 seconds)), 7))
    ),
    program = new DStreamFun[Int, (Size, Window, Int)] {
      override def apply[DS[_] : DStream : Windowed : Stateful]: DS[Int] => DS[(Size, Window, Int)] = {
        slidingSumsBySize(SlidingWindow(10 second, 5 seconds))
      }
    }
  )


  // sliding windows with concatenating lists, with a single global state
  def totalSlidingSums[DS[_] : DStream : Windowed](windowType: WindowType)(nums: DS[Int]): DS[List[Int]] = {
    nums
      .map(i => List(i))
      .windowReduce(windowType, _ => ())
  }

  val totalSlidingSumsTestCase = TestCase(
    input = Vector(
      Data(time = at(1 seconds), watermark = at(1 seconds), value = 1),
      Data(time = at(2 seconds), watermark = at(1 seconds), value = 2),
      Data(time = at(3 seconds), watermark = at(1 seconds), value = 3),
      Data(time = at(4 seconds), watermark = at(1 seconds), value = 4),
      Data(time = at(5 seconds), watermark = at(3 seconds), value = 5),
      Data(time = at(6 seconds), watermark = at(3 seconds), value = 6),
      Data(time = at(7 seconds), watermark = at(3 seconds), value = 7),
      Data(time = at(8 seconds), watermark = at(6 seconds), value = 8),
      Data(time = at(9 seconds), watermark = at(6 seconds), value = 9)
    ),
    output = Vector(
      Data(time = justBefore(2 seconds), watermark = at(1 seconds), value = List(1)),
      Data(time = justBefore(4 seconds), watermark = at(3 seconds), value = List(1, 2, 3)),
      Data(time = justBefore(6 seconds), watermark = at(3 seconds), value = List(1, 2, 3, 4, 5)),
      Data(time = justBefore(8 seconds), watermark = at(6 seconds), value = List(1, 2, 3, 4, 5, 6, 7)),
      Data(time = justBefore(10 seconds), watermark = at(6 seconds), value = List(1, 2, 3, 4, 5, 6, 7, 8, 9)),
      Data(time = justBefore(12 seconds), watermark = at(6 seconds), value = List(2, 3, 4, 5, 6, 7, 8, 9)),
      Data(time = justBefore(14 seconds), watermark = at(6 seconds), value = List(4, 5, 6, 7, 8, 9)),
      Data(time = justBefore(16 seconds), watermark = at(6 seconds), value = List(6, 7, 8, 9)),
      Data(time = justBefore(18 seconds), watermark = at(6 seconds), value = List(8, 9))
    ),
    program = new DStreamFun[Int, List[Int]] {
      override def apply[DS[_] : DStream : Windowed : Stateful]: DS[Int] => DS[List[Int]] = {
        totalSlidingSums(SlidingWindow(10 seconds, 2 seconds))
      }
    }
  )


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

  private def syncedData[T](time: Instant, value: T): Data[T] = Data(time, time, value)

  private def at(duration: Duration): Instant = Instant(100000L) + duration

  private def justBefore(duration: Duration): Instant = at(duration) - (1 milli)
}
