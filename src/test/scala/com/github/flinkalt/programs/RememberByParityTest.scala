package com.github.flinkalt.programs

import cats.data.State
import com.github.flinkalt.api._
import com.github.flinkalt.programs.utils.TestUtils.{runTestCaseWithFlink, runTestCaseWithMemory}
import com.github.flinkalt.programs.utils.{DStreamFun, TestCase, TestUtils}
import com.github.flinkalt.typeinfo.auto._
import org.scalatest.FunSuite
import shapeless._

//noinspection TypeAnnotation
class RememberByParityTest extends FunSuite {

  import Processing.ops._

  def rememberByParity[DS[_] : Processing](s1: DS[Int]): DS[String] = {

    type MyState = Int :: List[Int] :: HNil

    implicit def parityKey = Keyed.create[Int, Boolean](i => i % 2 == 0)

    s1.process1(i => State[Option[MyState], String](s0 => {
      val sum :: list :: HNil = s0.getOrElse(0 :: List() :: HNil)

      if (i < 0) {
        (None, s"Sum: $sum, list: $list")
      } else {
        (Some(sum + i :: (list :+ i) :: HNil), "Got it.")
      }
    }))

  }

  val testCase = TestCase(
    input = TestUtils.increasing(1, 2, 3, 4, 5, 6, -1, -2, 10, 11, -2, -1),
    output = TestUtils.increasingWithWatermarkLaggingBehind(
      "Got it.",
      "Got it.",
      "Got it.",
      "Got it.",
      "Got it.",
      "Got it.",
      "Sum: 9, list: List(1, 3, 5)",
      "Sum: 12, list: List(2, 4, 6)",
      "Got it.",
      "Got it.",
      "Sum: 10, list: List(10)",
      "Sum: 11, list: List(11)"
    ),
    program = new DStreamFun[Int, String] {
      override def apply[DS[_] : DStream : Windowed : Stateful : Processing]: DS[Int] => DS[String] = rememberByParity
    },
    deterministic = true
  )

  test("Remember the numbers by Flink") {
    runTestCaseWithFlink(testCase)
  }

  test("Remember the numbers by Memory") {
    runTestCaseWithMemory(testCase)
  }
}
