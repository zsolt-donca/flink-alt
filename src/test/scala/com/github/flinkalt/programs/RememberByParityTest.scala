package com.github.flinkalt.programs

import cats.data.State
import com.github.flinkalt.api._
import com.github.flinkalt.programs.utils.TestUtils.{runTestCaseWithFlink, runTestCaseWithFlink2, runTestCaseWithMemory, runTestCaseWithMemory2}
import com.github.flinkalt.programs.utils._
import com.github.flinkalt.typeinfo.auto._
import org.scalatest.FunSuite
import shapeless._

//noinspection TypeAnnotation
class RememberByParityTest extends FunSuite {

  import Processing.ops._

  def rememberByParity1[DS[_] : Processing](s1: DS[Int]): DS[String] = {

    type MyState = Int :: List[Int] :: HNil

    implicit def parityKey = Keyed.create[Int, Boolean](i => i % 2 == 0)

    s1.process1(i => State[Option[MyState], Vector[String]](s0 => {
      val sum :: list :: HNil = s0.getOrElse(0 :: List() :: HNil)

      if (i < 0) {
        (None, Vector(s"Sum: $sum, list: $list"))
      } else {
        (Some(sum + i :: (list :+ i) :: HNil), Vector("Got it."))
      }
    }))

  }

  val testCase1 = TestCase(
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
      override def apply[DS[_] : DStream : Windowed : Stateful : Processing]: DS[Int] => DS[String] = rememberByParity1
    },
    deterministic = true
  )

  def rememberByParity2[DS[_] : Processing](s1: DS[Int]): (DS[String], DS[Int]) = {

    type MyState = Int :: List[Int] :: HNil

    implicit def parityKey = Keyed.create[Int, Boolean](i => i % 2 == 0)

    s1.process2(i => State[Option[MyState], (Vector[String], Vector[Int])](s0 => {
      val sum :: list :: HNil = s0.getOrElse(0 :: List() :: HNil)

      if (i < 0) {
        (None, (Vector(s"Sum: $sum, list: $list"), Vector(0)))
      } else {
        (Some(sum + i :: (list :+ i) :: HNil), (Vector("Got it."), Vector(i * 2)))
      }
    }))
  }

  val testCase2 = TestCase2(
    input = TestUtils.increasing(1, 2, 3, 4, 5, 6, -1, -2, 10, 11, -2, -1),
    output1 = TestUtils.increasingWithWatermarkLaggingBehind(
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
    output2 = TestUtils.increasingWithWatermarkLaggingBehind(
      2,
      4,
      6,
      8,
      10,
      12,
      0,
      0,
      20,
      22,
      0,
      0
    ),
    program = new DStreamFun2[Int, String, Int] {
      override def apply[DS[_] : DStream : Windowed : Stateful : Processing]: DS[Int] => (DS[String], DS[Int]) = rememberByParity2
    },
    deterministic = true
  )

  test("Remember the numbers 1 by Flink") {
    runTestCaseWithFlink(testCase1)
  }

  test("Remember the numbers 1 by Memory") {
    runTestCaseWithMemory(testCase1)
  }

  test("Remember the numbers 2 by Flink") {
    runTestCaseWithFlink2(testCase2)
  }

  test("Remember the numbers 2 by Memory") {
    runTestCaseWithMemory2(testCase2)
  }
}
