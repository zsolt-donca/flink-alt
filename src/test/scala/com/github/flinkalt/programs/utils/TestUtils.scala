package com.github.flinkalt.programs.utils

import cats.Order
import cats.instances.long._
import cats.instances.string._
import com.github.flinkalt.flink._
import com.github.flinkalt.flink.helper.{DataStreamCollector, _}
import com.github.flinkalt.memory.{DataAndWatermark, MemoryStream}
import com.github.flinkalt.time._
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.auto._
import com.github.flinkalt.{DStream, Stateful, Windowed}
import org.apache.flink.streaming.api.TimeCharacteristic.EventTime
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.scalatest.Assertions

// necessary boilerplate, Scala does not have built-in the concept of polymorphic functions
// also, it turns out there can be many ways a function can be "polymorphic"
// this is neither what shapeless nor what cats calls polymorphic functions
trait DStreamFun[A, B] {
  def apply[DS[_] : DStream : Windowed : Stateful]: DS[A] => DS[B]
}

case class TestCase[A, B]
(
  input: Vector[DataAndWatermark[A]],
  output: Vector[DataAndWatermark[B]],
  program: DStreamFun[A, B],
  deterministic: Boolean = false
)


object TestUtils extends Assertions {

  def numberAtSeconds(i: Int): DataAndWatermark[Int] = syncedData(at(i seconds), i)

  def syncedData[T](time: Instant, value: T): DataAndWatermark[T] = DataAndWatermark(time, time, value)

  def at(duration: Duration): Instant = Instant(60 * 60 * 1000) + duration

  def justBefore(duration: Duration): Instant = at(duration) - (1 milli)


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

    val vector = testCase.input

    val stream = createFlinkSource(env, vector)
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
