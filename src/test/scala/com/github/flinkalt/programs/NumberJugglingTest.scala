package com.github.flinkalt.programs

import com.github.flinkalt.memory.DataAndWatermark
import com.github.flinkalt.programs.utils.TestUtils.{at, runTestCaseWithFlink, runTestCaseWithMemory, syncedData}
import com.github.flinkalt.programs.utils.{DStreamFun, TestCase}
import com.github.flinkalt.time.{Instant, _}
import com.github.flinkalt.typeinfo.auto._
import com.github.flinkalt.{DStream, Stateful, Windowed}
import org.scalatest.FunSuite

class NumberJugglingTest extends FunSuite {

  import DStream.ops._

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
      DataAndWatermark(time = at(0 seconds), watermark = Instant.minValue, "odd: 5"),
      DataAndWatermark(time = at(0 seconds), watermark = Instant.minValue, "even: 2"),
      DataAndWatermark(time = at(1 seconds), watermark = at(0 seconds), "odd: 15"),
      DataAndWatermark(time = at(1 seconds), watermark = at(0 seconds), "odd: 7"),
      DataAndWatermark(time = at(3 seconds), watermark = at(2 seconds), "even: 10"),
      DataAndWatermark(time = at(3 seconds), watermark = at(2 seconds), "odd: 5")
    ),
    program = new DStreamFun[Int, String] {
      override def apply[DS[_] : DStream : Windowed : Stateful]: DS[Int] => DS[String] = numberJuggling
    }
  )


  test("Number Juggling with Flink") {
    runTestCaseWithFlink(numberJugglingTestCase)
  }

  test("Number Juggling with Memory") {
    runTestCaseWithMemory(numberJugglingTestCase)
  }
}
