package com.github.flinkalt.programs

import cats.instances.string._
import com.github.flinkalt.api._
import com.github.flinkalt.memory.DataAndWatermark
import com.github.flinkalt.programs.FizzBuzzProgram.fizzBuzzTestCase
import com.github.flinkalt.programs.utils.TestUtils._
import com.github.flinkalt.programs.utils.{DStreamFun, TestCase}
import com.github.flinkalt.time._
import com.github.flinkalt.typeinfo.auto._
import org.scalatest.FunSuite

object FizzBuzzProgram {

  import DStream.ops._
  import Windowed.ops._

  def fizzBuzzProgram[DS[_] : DStream : Windowed](number: DS[Int]): DS[String] = {
    val fizzes = number.filter(i => i % 3 == 0 && i % 5 != 0).map(_ => "Fizz")
    val buzzes = number.filter(i => i % 3 != 0 && i % 5 == 0).map(_ => "Buzz")
    val fizzBuzzes = number.filter(i => i % 3 == 0 && i % 5 == 0).map(_ => "FizzBuzz")
    val numbers = number.filter(i => i % 3 != 0 && i % 5 != 0).map(_.toString)

    val unioned: DS[String] = fizzes.union(buzzes).union(fizzBuzzes).union(numbers)

    // sorts values by time
    // works because values will be mapped exactly to one window (and each window will have exactly one value)
    unioned.windowReduce(WindowTypes.Tumbling(1 second), _ => ())
  }

  val fizzBuzzTestCase = TestCase(
    input = (1 to 20).map(numberAtSeconds).toVector,

    // watermark lags behind when doing simple transformations as well, without any effect
    output = Vector(
      DataAndWatermark(time = justBefore(2 seconds), watermark = at(1 seconds), "1"),
      DataAndWatermark(time = justBefore(3 seconds), watermark = at(2 seconds), "2"),
      DataAndWatermark(time = justBefore(4 seconds), watermark = at(3 seconds), "Fizz"),
      DataAndWatermark(time = justBefore(5 seconds), watermark = at(4 seconds), "4"),
      DataAndWatermark(time = justBefore(6 seconds), watermark = at(5 seconds), "Buzz"),
      DataAndWatermark(time = justBefore(7 seconds), watermark = at(6 seconds), "Fizz"),
      DataAndWatermark(time = justBefore(8 seconds), watermark = at(7 seconds), "7"),
      DataAndWatermark(time = justBefore(9 seconds), watermark = at(8 seconds), "8"),
      DataAndWatermark(time = justBefore(10 seconds), watermark = at(9 seconds), "Fizz"),
      DataAndWatermark(time = justBefore(11 seconds), watermark = at(10 seconds), "Buzz"),
      DataAndWatermark(time = justBefore(12 seconds), watermark = at(11 seconds), "11"),
      DataAndWatermark(time = justBefore(13 seconds), watermark = at(12 seconds), "Fizz"),
      DataAndWatermark(time = justBefore(14 seconds), watermark = at(13 seconds), "13"),
      DataAndWatermark(time = justBefore(15 seconds), watermark = at(14 seconds), "14"),
      DataAndWatermark(time = justBefore(16 seconds), watermark = at(15 seconds), "FizzBuzz"),
      DataAndWatermark(time = justBefore(17 seconds), watermark = at(16 seconds), "16"),
      DataAndWatermark(time = justBefore(18 seconds), watermark = at(17 seconds), "17"),
      DataAndWatermark(time = justBefore(19 seconds), watermark = at(18 seconds), "Fizz"),
      DataAndWatermark(time = justBefore(20 seconds), watermark = at(19 seconds), "19"),
      DataAndWatermark(time = justBefore(21 seconds), watermark = at(20 seconds), "Buzz")
    ),
    program = new DStreamFun[Int, String] {
      override def apply[DS[_] : DStream : Windowed : Stateful : Processing]: DS[Int] => DS[String] = fizzBuzzProgram
    },
    deterministic = true
  )

}

class FizzBuzzTest extends FunSuite {
  test("FizzBuzz program with Flink") {
    runTestCaseWithFlink(fizzBuzzTestCase)
  }

  test("FizzBuzz program with Memory") {
    runTestCaseWithMemory(fizzBuzzTestCase)
  }
}