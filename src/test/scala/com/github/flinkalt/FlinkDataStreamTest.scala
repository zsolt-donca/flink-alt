package com.github.flinkalt

import com.github.flinkalt.flink.helper._
import com.github.flinkalt.memory.Data
import com.github.flinkalt.time.Instant
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
      syncedData(timeAt(0), "x"),
      syncedData(timeAt(1), "y z"),
      syncedData(timeAt(2), ""),
      syncedData(timeAt(5), "z q y y")
    )

    val stream = createSource(env, vector)
    val outStream = TestPrograms.totalWordCount(stream)
    val collector = outStream.collect()

    env.execute()

    val actual = collector.toList
    assert(actual == List(
      Count("x", 1),
      Count("y", 1),
      Count("z", 1),
      Count("z", 2),
      Count("q", 1),
      Count("y", 2),
      Count("y", 3)
    ))
  }

  private def createSource(env: StreamExecutionEnvironment, vector: Vector[Data[String]]) = {
    env.fromCollection(vector)
      .assignTimestampsAndWatermarks(dataTimestampExtractor)
      .map(data => data.value)
  }

  private def dataTimestampExtractor: AssignerWithPunctuatedWatermarks[Data[String]] = {
    new AssignerWithPunctuatedWatermarks[Data[String]] {
      override def checkAndGetNextWatermark(lastElement: Data[String], extractedTimestamp: Long): Watermark = {
        new Watermark(lastElement.watermark.millis)
      }

      override def extractTimestamp(element: Data[String], previousElementTimestamp: Long): Long = {
        element.time.millis
      }
    }
  }
  private def syncedData[T](time: Instant, value: T): Data[T] = Data(time, time, value)

  private def timeAt(i: Int): Instant = Instant(1000L + i)

}
