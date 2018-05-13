package com.github.flinkalt

import cats.Order
import cats.instances.long._
import cats.instances.string._
import com.github.flinkalt.memory.MemoryStream
import org.scalatest.FunSuite

class MemoryStreamTest extends FunSuite {

  implicit def anyToTypeInfo[T]: TypeInfo[T] = null

  test("Number Juggling") {
    runTestCase(TestPrograms.numberJugglingTestCase)
  }

  test("Total Word Count") {
    runTestCase(TestPrograms.totalWordCountTestCase)
  }

  test("Sliding Word Count") {
    runTestCase(TestPrograms.slidingWordCountTestCase)
  }

  test("Sliding numbers with late watermarks") {
    runTestCase(TestPrograms.totalSlidingSumsTestCase)
  }

  test("Sliding number ladder") {
    runTestCase(TestPrograms.slidingSumsBySizeTestCase)
  }

  private def runTestCase[A: TypeInfo, B: TypeInfo](testCase: TestCase[A, B]): Unit = {

    val stream = MemoryStream.fromData(testCase.input)
    val outStream = testCase.program[MemoryStream].apply(stream)
    val actual = outStream.toData

    implicit def dataOrder[T]: Order[Data[T]] = Order.whenEqual(Order.by(_.time.millis), Order.by(_.value.toString))
    implicit def toOrdering[T: Order]: Ordering[T] = Order[T].toOrdering

    assert(actual.sorted == testCase.output.sorted)
  }
}
