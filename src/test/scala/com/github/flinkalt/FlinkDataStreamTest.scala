package com.github.flinkalt

import com.github.flinkalt.flink.helper._
import com.github.flinkalt.memory.Data
import org.apache.flink.streaming.api.TimeCharacteristic.EventTime
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.scalatest.FunSuite

class FlinkDataStreamTest extends FunSuite {

  import com.github.flinkalt.flink._

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
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(EventTime)
    implicit val dsCollector: DataStreamCollector = new DataStreamCollector

    val vector = testCase.input

    val stream = createSource(env, vector)
    val outStream = testCase.program[DataStream].apply(stream)
    val collector = outStream.collect()

    env.execute()

    val actual = collector.toVector
    assert(actual == testCase.output)
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
}
