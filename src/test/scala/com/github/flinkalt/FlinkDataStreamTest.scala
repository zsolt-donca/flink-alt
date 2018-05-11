package com.github.flinkalt

import com.github.flinkalt.flink.helper._
import com.github.flinkalt.memory.Data
import com.github.flinkalt.time._
import org.apache.flink.streaming.api.TimeCharacteristic.EventTime
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.scalatest.FunSuite

class FlinkDataStreamTest extends FunSuite {

  import com.github.flinkalt.flink._

  test("Total Word Count") {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(EventTime)
    implicit val dsCollector: DataStreamCollector = new DataStreamCollector

    val vector = Vector(
      syncedData(at(0 seconds), "x"),
      syncedData(at(1 seconds), "y z"),
      syncedData(at(2 seconds), ""),
      syncedData(at(5 seconds), "z q y y")
    )

    val stream = createSource(env, vector)
    val outStream = TestPrograms.totalWordCount(stream)
    val collector = outStream.collect()

    env.execute()

    // watermarks are always lagging behind, coming from the previous value (and thus Long.MinValue for the first)
    val actual = collector.toVector
    assert(actual == Vector(
      Data(time = at(0 seconds), watermark = Instant(Long.MinValue), Count("x", 1)),
      Data(time = at(1 seconds), watermark = at(0 seconds), Count("y", 1)),
      Data(time = at(1 seconds), watermark = at(0 seconds), Count("z", 1)),
      Data(time = at(5 seconds), watermark = at(2 seconds), Count("z", 2)),
      Data(time = at(5 seconds), watermark = at(2 seconds), Count("q", 1)),
      Data(time = at(5 seconds), watermark = at(2 seconds), Count("y", 2)),
      Data(time = at(5 seconds), watermark = at(2 seconds), Count("y", 3))
    ))
  }

  test("Sliding Word Count") {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(EventTime)
    implicit val dsCollector: DataStreamCollector = new DataStreamCollector

    val vector = Vector(
      syncedData(at(0 seconds), "x"),
      syncedData(at(2 seconds), "y z"),
      syncedData(at(3 seconds), "y"),
      syncedData(at(6 seconds), "z q y y")
    )

    val stream = createSource(env, vector)
    val outStream = TestPrograms.slidingWordCount(stream, SlidingWindow(4 seconds, 2 seconds))
    val collector = outStream.collect()

    env.execute()

    val actual = collector.toVector
    assert(actual == Vector(
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
    ))
  }

  test("Sliding numbers with late watermarks") {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(EventTime)
    implicit val dsCollector: DataStreamCollector = new DataStreamCollector

    val vector = Vector(
      Data(time = at(1 seconds), watermark = at(1 seconds), value = 1),
      Data(time = at(2 seconds), watermark = at(1 seconds), value = 2),
      Data(time = at(3 seconds), watermark = at(1 seconds), value = 3),
      Data(time = at(4 seconds), watermark = at(1 seconds), value = 4),
      Data(time = at(5 seconds), watermark = at(3 seconds), value = 5),
      Data(time = at(6 seconds), watermark = at(3 seconds), value = 6),
      Data(time = at(7 seconds), watermark = at(3 seconds), value = 7),
      Data(time = at(8 seconds), watermark = at(6 seconds), value = 8),
      Data(time = at(9 seconds), watermark = at(6 seconds), value = 9)
    )

    val stream = createSource(env, vector)
    val outStream = TestPrograms.totalSlidingSums(stream, SlidingWindow(10 seconds, 2 seconds))
    val collector = outStream.collect()

    env.execute()

    val actual = collector.toVector
    assert(actual == Vector(
      Data(time = justBefore(2 seconds), watermark = at(1 seconds), value = List(1)),
      Data(time = justBefore(4 seconds), watermark = at(3 seconds), value = List(1, 2, 3)),
      Data(time = justBefore(6 seconds), watermark = at(3 seconds), value = List(1, 2, 3, 4, 5)),
      Data(time = justBefore(8 seconds), watermark = at(6 seconds), value = List(1, 2, 3, 4, 5, 6, 7)),
      Data(time = justBefore(10 seconds), watermark = at(6 seconds), value = List(1, 2, 3, 4, 5, 6, 7, 8, 9)),
      Data(time = justBefore(12 seconds), watermark = at(6 seconds), value = List(2, 3, 4, 5, 6, 7, 8, 9)),
      Data(time = justBefore(14 seconds), watermark = at(6 seconds), value = List(4, 5, 6, 7, 8, 9)),
      Data(time = justBefore(16 seconds), watermark = at(6 seconds), value = List(6, 7, 8, 9)),
      Data(time = justBefore(18 seconds), watermark = at(6 seconds), value = List(8, 9))
    ))
  }

  test("Sliding number ladder") {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(EventTime)
    implicit val dsCollector: DataStreamCollector = new DataStreamCollector

    val vector = Vector(
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
    )

    val stream = createSource(env, vector)
    val outStream = TestPrograms.slidingSumsBySize(stream, SlidingWindow(10 second, 5 seconds))
    val collector = outStream.collect()

    env.execute()

    val actual = collector.toVector
    assert(actual == Vector(
      Data(time = justBefore(5 seconds), watermark = at(3 seconds), (Small, Window(start = at(-5 seconds), end = at(5 seconds)), 1 + 2 + 3 + 3)),
      Data(time = justBefore(5 seconds), watermark = at(3 seconds), (Large, Window(start = at(-5 seconds), end = at(5 seconds)), 10 + 12)),

      Data(time = justBefore(10 seconds), watermark = at(7 seconds), (Small, Window(start = at(0 seconds), end = at(10 seconds)), 1 + 2 + 3 + 3 + 4)),
      Data(time = justBefore(10 seconds), watermark = at(7 seconds), (Large, Window(start = at(0 seconds), end = at(10 seconds)), 10 + 12 + 13)),

      Data(time = justBefore(15 seconds), watermark = at(12 seconds), (Large, Window(start = at(5 seconds), end = at(15 seconds)), 13 + 11)),
      Data(time = justBefore(15 seconds), watermark = at(12 seconds), (Small, Window(start = at(5 seconds), end = at(15 seconds)), 4 + 7)),

      Data(time = justBefore(20 seconds), watermark = at(12 seconds), (Large, Window(start = at(10 seconds), end = at(20 seconds)), 11)),
      Data(time = justBefore(20 seconds), watermark = at(12 seconds), (Small, Window(start = at(10 seconds), end = at(20 seconds)), 7))
    ))
  }

  private def createSource[A: TypeInfo](env: StreamExecutionEnvironment, vector: Vector[Data[A]]): DataStream[A] = {
    env.fromCollection(vector)
      .assignTimestampsAndWatermarks(dataTimestampExtractor[A])
      .map(data => data.value)
  }

  private def dataTimestampExtractor[A]: AssignerWithPunctuatedWatermarks[Data[A]] = {
    new AssignerWithPunctuatedWatermarks[Data[A]] {
      override def extractTimestamp(element: Data[A], previousElementTimestamp: Long): Long = {
        element.time.millis
      }

      override def checkAndGetNextWatermark(lastElement: Data[A], extractedTimestamp: Long): Watermark = {
        new Watermark(lastElement.watermark.millis)
      }
    }
  }

  private def syncedData[T](time: Instant, value: T): Data[T] = Data(time, time, value)

  private def at(duration: Duration): Instant = Instant(100000L) + duration

  private def justBefore(duration: Duration): Instant = at(duration) - (1 milli)

}
