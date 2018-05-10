package com.github.flinkalt

import cats.data.State
import com.github.flinkalt.memory.{Data, MemoryDStream}
import com.github.flinkalt.time.Instant
import org.scalatest.FunSuite

class WordCountTest extends FunSuite {

  import DStream.ops._

  test("Word count") {
    def wordCount[DS[_] : DStream](lines: DS[String]): DS[(String, Int)] = {
      def splitToWords(line: String): Seq[String] = {
        line.toLowerCase().split("\\W+").filter(_.nonEmpty)
      }

      def counting[T]: StateTrans[T, Int, T, (T, Int)] = StateTrans(
        identity,
        t => State(maybeCount => {
          val count = maybeCount.getOrElse(0) + 1
          (Some(count), (t, count))
        }))

      lines.flatMap(splitToWords).mapWithState(counting)
    }

    def data[T](time: Instant, value: T): Data[T] = Data(time, time, value)
    def timeAt(i: Int): Instant = Instant(1000L + i)

    val stream = MemoryDStream(Vector(
      data(timeAt(0), "x"),
      data(timeAt(1), "y z"),
      data(timeAt(2), ""),
      data(timeAt(5), "z q y y")
    ))
    val outStream: MemoryDStream[(String, Int)] = wordCount(stream)

    val actualValues = outStream.vector
    assert(actualValues == Vector(
      data(timeAt(0), ("x", 1)),
      data(timeAt(1), ("y", 1)),
      data(timeAt(1), ("z", 1)),
      data(timeAt(5), ("z", 2)),
      data(timeAt(5), ("q", 1)),
      data(timeAt(5), ("y", 2)),
      data(timeAt(5), ("y", 3))
    ))
  }
}
