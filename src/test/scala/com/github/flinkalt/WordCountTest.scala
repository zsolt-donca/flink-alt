package com.github.flinkalt

import cats.data.State
import com.github.flinkalt.memory.MemoryDStream
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

    val stream = MemoryDStream(Vector("x", "y", "z", "y"))
    val outStream: MemoryDStream[(String, Int)] = wordCount(stream)

    assert(outStream.vector == Vector(("x", 1), ("y", 1), ("z", 1), ("y", 2)))
  }
}
