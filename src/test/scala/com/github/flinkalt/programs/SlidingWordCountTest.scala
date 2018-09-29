package com.github.flinkalt.programs

import cats.kernel.Semigroup
import com.github.flinkalt.api._
import com.github.flinkalt.memory.DataAndWatermark
import com.github.flinkalt.programs.utils.TestUtils._
import com.github.flinkalt.programs.utils._
import com.github.flinkalt.time._
import com.github.flinkalt.typeinfo.auto._
import org.scalatest.FunSuite

object SlidingWordCountProgram {

  import com.github.flinkalt.api.DStream.ops._
  import com.github.flinkalt.api.Windowed.ops._

  def slidingWordCount[DS[_] : DStream : Windowed](windowType: WindowType)(lines: DS[String]): DS[Count[String]] = {
    lines
      .flatMap(splitToWords)
      .map(s => Count(s, 1))
      .windowReduce(windowType, _.value)
  }

  def splitToWords(line: String): Seq[String] = {
    line.toLowerCase().split("\\W+").filter(_.nonEmpty)
  }

  //noinspection ConvertExpressionToSAM
  implicit def countSemigroup[T]: Semigroup[Count[T]] = new Semigroup[Count[T]] {
    override def combine(x: Count[T], y: Count[T]): Count[T] = Count(y.value, x.count + y.count)
  }
}

class SlidingWordCountTest extends FunSuite {
  // import the method syntax for the type classes
  val slidingWordCountTestCase = TestCase(
    input = Vector(
      syncedData(at(0 seconds), "x"),
      syncedData(at(2 seconds), "y z"),
      syncedData(at(3 seconds), "y"),
      syncedData(at(6 seconds), "z q y y")
    ),
    output = Vector(
      DataAndWatermark(time = justBefore(2 seconds), watermark = at(0 seconds), Count("x", 1)),

      DataAndWatermark(time = justBefore(4 seconds), watermark = at(3 seconds), Count("x", 1)),
      DataAndWatermark(time = justBefore(4 seconds), watermark = at(3 seconds), Count("y", 2)),
      DataAndWatermark(time = justBefore(4 seconds), watermark = at(3 seconds), Count("z", 1)),

      DataAndWatermark(time = justBefore(6 seconds), watermark = at(3 seconds), Count("y", 2)),
      DataAndWatermark(time = justBefore(6 seconds), watermark = at(3 seconds), Count("z", 1)),

      DataAndWatermark(time = justBefore(8 seconds), watermark = at(6 seconds), Count("y", 2)),
      DataAndWatermark(time = justBefore(8 seconds), watermark = at(6 seconds), Count("q", 1)),
      DataAndWatermark(time = justBefore(8 seconds), watermark = at(6 seconds), Count("z", 1)),

      DataAndWatermark(time = justBefore(10 seconds), watermark = at(6 seconds), Count("q", 1)),
      DataAndWatermark(time = justBefore(10 seconds), watermark = at(6 seconds), Count("y", 2)),
      DataAndWatermark(time = justBefore(10 seconds), watermark = at(6 seconds), Count("z", 1))
    ),
    program = new DStreamFun[String, Count[String]] {
      override def apply[DS[_] : DStream : Windowed : Stateful]: DS[String] => DS[Count[String]] = {
        SlidingWordCountProgram.slidingWordCount(WindowTypes.Sliding(4 seconds, 2 seconds))
      }
    }
  )

  test("Sliding Word Count with Flink") {
    runTestCaseWithFlink(slidingWordCountTestCase)
  }

  test("Sliding Word Count with Memory") {
    runTestCaseWithMemory(slidingWordCountTestCase)
  }

}
