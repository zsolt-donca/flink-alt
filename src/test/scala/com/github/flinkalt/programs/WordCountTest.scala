package com.github.flinkalt.programs

import cats.data.State
import com.github.flinkalt.Stateful.StateTrans
import com.github.flinkalt.memory.DataAndWatermark
import com.github.flinkalt.programs.utils.TestUtils.{at, runTestCaseWithFlink, runTestCaseWithMemory, syncedData}
import com.github.flinkalt.programs.utils.{DStreamFun, TestCase}
import com.github.flinkalt.time.{Instant, _}
import com.github.flinkalt.typeinfo.auto._
import com.github.flinkalt.{DStream, Keyed, Stateful, Windowed}
import org.scalatest.FunSuite

case class Count[T](value: T, count: Int)

object WordCountProgram {

  import DStream.ops._
  import Stateful.ops._

  // word count using a stateful transformation counting each element, each value having an individual state
  def totalWordCount[DS[_] : DStream : Stateful](lines: DS[String]): DS[Count[String]] = {
    implicit val stringKeys: Keyed[String] = Keyed.create[String, String](identity)

    lines
      .flatMap(splitToWords)
      .mapWithState(zipWithCount)
  }

  def splitToWords(line: String): Seq[String] = {
    line.toLowerCase().split("\\W+").filter(_.nonEmpty)
  }

  def zipWithCount[T]: StateTrans[Int, T, Count[T]] = {
    t =>
      State(maybeCount => {
        val count = maybeCount.getOrElse(0) + 1
        (Some(count), Count(t, count))
      })
  }
}

class WordCount extends FunSuite with Serializable {
  // import the method syntax for the type classes
  val totalWordCountTestCase = TestCase(
    input = Vector(
      syncedData(at(0 seconds), "x"),
      syncedData(at(1 seconds), "y z"),
      syncedData(at(2 seconds), ""),
      syncedData(at(5 seconds), "z q y y")
    ),
    // watermarks are always lagging behind, coming from the previous value (and thus Long.MinValue for the first)
    output = Vector(
      DataAndWatermark(time = at(0 seconds), watermark = Instant.minValue, Count("x", 1)),
      DataAndWatermark(time = at(1 seconds), watermark = at(0 seconds), Count("y", 1)),
      DataAndWatermark(time = at(1 seconds), watermark = at(0 seconds), Count("z", 1)),
      DataAndWatermark(time = at(5 seconds), watermark = at(2 seconds), Count("z", 2)),
      DataAndWatermark(time = at(5 seconds), watermark = at(2 seconds), Count("q", 1)),
      DataAndWatermark(time = at(5 seconds), watermark = at(2 seconds), Count("y", 2)),
      DataAndWatermark(time = at(5 seconds), watermark = at(2 seconds), Count("y", 3))
    ),
    program = new DStreamFun[String, Count[String]] {
      override def apply[DS[_] : DStream : Windowed : Stateful]: DS[String] => DS[Count[String]] = WordCountProgram.totalWordCount
    }
  )

  test("Total Word Count with Flink") {
    runTestCaseWithFlink(totalWordCountTestCase)
  }

  test("Total Word Count with Memory") {
    runTestCaseWithMemory(totalWordCountTestCase)
  }
}
