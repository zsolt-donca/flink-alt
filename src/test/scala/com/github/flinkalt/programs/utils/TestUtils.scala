package com.github.flinkalt.programs.utils

import cats.Order
import cats.instances.long._
import cats.instances.string._
import com.github.flinkalt.api.{DStream, Processing, Stateful, Windowed}
import com.github.flinkalt.flink._
import com.github.flinkalt.flink.helper.{DataStreamCollector, _}
import com.github.flinkalt.memory.{DataAndWatermark, MemoryStream}
import com.github.flinkalt.time._
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.streaming.api.TimeCharacteristic.EventTime
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.scalatest.Assertions

trait DStreamFun[A, B] {
  def apply[DS[_] : DStream : Windowed : Stateful : Processing]: DS[A] => DS[B]
}

case class TestCase[A, B]
(
  input: Vector[DataAndWatermark[A]],
  output: Vector[DataAndWatermark[B]],
  program: DStreamFun[A, B],
  deterministic: Boolean = false
)

trait DStreamFun2[A, B1, B2] {
  def apply[DS[_] : DStream : Windowed : Stateful : Processing]: DS[A] => (DS[B1], DS[B2])
}

case class TestCase2[A, B1, B2]
(
  input: Vector[DataAndWatermark[A]],
  output1: Vector[DataAndWatermark[B1]],
  output2: Vector[DataAndWatermark[B2]],
  program: DStreamFun2[A, B1, B2],
  deterministic: Boolean = false
)


object TestUtils extends Assertions {

  def numberAtSeconds(i: Int): DataAndWatermark[Int] = syncedData(at(i seconds), i)

  def syncedData[T](time: Instant, value: T): DataAndWatermark[T] = DataAndWatermark(time, time, value)

  def at(duration: Duration): Instant = Instant(60 * 60 * 1000) + duration

  def justBefore(duration: Duration): Instant = at(duration) - (1 milli)

  def increasing[A](values: A*): Vector[DataAndWatermark[A]] = {
    values.zipWithIndex.map({
      case (value, i) =>
        val time = at(i seconds)
        DataAndWatermark(time, time, value)
    }).toVector
  }

  def increasingWithWatermarkLaggingBehind[A](values: A*): Vector[DataAndWatermark[A]] = {
    values.zipWithIndex.map({
      case (value, i) =>
        val time = at(i seconds)
        val wm = if (i == 0) Instant.minValue else at((i - 1) seconds)
        DataAndWatermark(time, wm, value)
    }).toVector
  }

  def runTestCaseWithMemory[A: TypeInfo, B: TypeInfo](testCase: TestCase[A, B]): Unit = {
    val stream = MemoryStream.fromData(testCase.input)
    val outStream = testCase.program[MemoryStream].apply(stream)
    val actual = outStream.toPostData

    if (testCase.deterministic) {
      assert(actual == testCase.output)
    } else {
      assert(actual.sorted == testCase.output.sorted)
    }
  }

  def runTestCaseWithFlink[A: TypeInfo, B: TypeInfo](testCase: TestCase[A, B]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(EventTime)
    implicit val dsCollector: DataStreamCollector = new DataStreamCollector

    val stream = createFlinkSource(env, testCase.input)
    val outStream = testCase.program[DataStream].apply(stream)
    val collector = outStream.collect()

    env.execute()

    val actual = collector.toVector

    if (testCase.deterministic) {
      assert(actual == testCase.output)
    } else {
      assert(actual.sorted == testCase.output.sorted)
    }
  }

  def runTestCaseWithMemory2[A: TypeInfo, B1: TypeInfo, B2: TypeInfo](testCase: TestCase2[A, B1, B2]): Unit = {
    val stream = MemoryStream.fromData(testCase.input)
    val (outStream1, outStream2) = testCase.program[MemoryStream].apply(stream)
    val actual1 = outStream1.toPostData
    val actual2 = outStream2.toPostData

    if (testCase.deterministic) {
      assert(actual1 == testCase.output1)
      assert(actual2 == testCase.output2)
    } else {
      assert(actual1.sorted == testCase.output1.sorted)
      assert(actual2.sorted == testCase.output2.sorted)
    }
  }

  def runTestCaseWithFlink2[A: TypeInfo, B1: TypeInfo, B2: TypeInfo](testCase: TestCase2[A, B1, B2]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(EventTime)
    implicit val dsCollector: DataStreamCollector = new DataStreamCollector

    val stream = createFlinkSource(env, testCase.input)
    val (outStream1, outStream2) = testCase.program[DataStream].apply(stream)
    val collector1 = outStream1.collect()
    val collector2 = outStream2.collect()

    env.execute()

    val actual1 = collector1.toVector
    val actual2 = collector2.toVector

    if (testCase.deterministic) {
      assert(actual1 == testCase.output1)
      assert(actual2 == testCase.output2)
    } else {
      assert(actual1.sorted == testCase.output1.sorted)
      assert(actual2.sorted == testCase.output2.sorted)
    }
  }

  private implicit def dataOrder[T]: Order[DataAndWatermark[T]] = Order.whenEqual(Order.by(_.time.millis), Order.by(_.value.toString))

  private implicit def toOrdering[T: Order]: Ordering[T] = Order[T].toOrdering

  private def createFlinkSource[A: TypeInfo](env: StreamExecutionEnvironment, vector: Vector[DataAndWatermark[A]]): DataStream[A] = {
    env.fromCollection(vector)
      .assignTimestampsAndWatermarks(dataTimestampExtractor[A])
      .map(data => data.value)
  }

  private def dataTimestampExtractor[A]: AssignerWithPunctuatedWatermarks[DataAndWatermark[A]] = {
    new AssignerWithPunctuatedWatermarks[DataAndWatermark[A]] {
      override def extractTimestamp(element: DataAndWatermark[A], previousElementTimestamp: Long): Long = {
        element.time.millis
      }

      override def checkAndGetNextWatermark(lastElement: DataAndWatermark[A], extractedTimestamp: Long): Watermark = {
        new Watermark(lastElement.watermark.millis)
      }
    }
  }

}
