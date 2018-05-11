package com.github.flinkalt

import com.github.flinkalt.memory.{Data, MemoryStream}
import com.github.flinkalt.time._
import org.scalatest.FunSuite

class MemoryStreamTest extends FunSuite {

  implicit def anyToTypeInfo[T]: TypeInfo[T] = null

  test("Total Word Count") {

    val stream = MemoryStream(Vector(
      syncedData(timeAt(0), "x"),
      syncedData(timeAt(1), "y z"),
      syncedData(timeAt(2), ""),
      syncedData(timeAt(5), "z q y y")
    ))

    val outStream: MemoryStream[Count[String]] = TestPrograms.totalWordCount(stream)

    val actualValues = outStream.vector
    assert(actualValues == Vector(
      syncedData(timeAt(0), Count("x", 1)),
      syncedData(timeAt(1), Count("y", 1)),
      syncedData(timeAt(1), Count("z", 1)),
      syncedData(timeAt(5), Count("z", 2)),
      syncedData(timeAt(5), Count("q", 1)),
      syncedData(timeAt(5), Count("y", 2)),
      syncedData(timeAt(5), Count("y", 3))
    ))
  }

  test("Sliding Word Count") {
    val stream = MemoryStream(Vector(
      syncedData(timeAt(0), "x"),
      syncedData(timeAt(2), "y z"),
      syncedData(timeAt(3), "y"),
      syncedData(timeAt(6), "z q y y")
    ))

    val outStream: MemoryStream[Count[String]] = TestPrograms.slidingWordCount(SlidingWindow(4 seconds, 2 seconds))(stream)

    val actualValues = outStream.vector
    assert(actualValues == Vector(
      Data(time = timeAt(2), watermark = timeAt(2), Count("x", 1)),

      Data(time = timeAt(4), watermark = timeAt(6), Count("y", 2)),
      Data(time = timeAt(4), watermark = timeAt(6), Count("x", 1)),
      Data(time = timeAt(4), watermark = timeAt(6), Count("z", 1)),

      Data(time = timeAt(6), watermark = timeAt(6), Count("z", 1)),
      Data(time = timeAt(6), watermark = timeAt(6), Count("y", 2)),

      Data(time = timeAt(8), watermark = timeAt(8), Count("q", 1)),
      Data(time = timeAt(8), watermark = timeAt(8), Count("z", 1)),
      Data(time = timeAt(8), watermark = timeAt(8), Count("y", 2)),

      Data(time = timeAt(10), watermark = timeAt(10), Count("q", 1)),
      Data(time = timeAt(10), watermark = timeAt(10), Count("z", 1)),
      Data(time = timeAt(10), watermark = timeAt(10), Count("y", 2))
    ))
  }

  test("Sliding number ladder") {
    val stream = MemoryStream(Vector(
      syncedData(timeAt(0), 1),
      syncedData(timeAt(0), 2),
      syncedData(timeAt(0), 10),
      syncedData(timeAt(0), 12),

      syncedData(timeAt(3), 3),
      syncedData(timeAt(3), 3),

      syncedData(timeAt(7), 4),
      syncedData(timeAt(7), 13),

      syncedData(timeAt(11), 11),
      syncedData(timeAt(12), 7)
    ))

    val outStream: MemoryStream[(Size, Window, Int)] = TestPrograms.slidingSumsBySize(SlidingWindow(10 seconds, 5 seconds))(stream)
    val actualValues = outStream.vector
    assert(actualValues == Vector(
      Data(time = timeAt(5), watermark = timeAt(7), (Large, Window(start = timeAt(-5), end = timeAt(5)), 10 + 12)),
      Data(time = timeAt(5), watermark = timeAt(7), (Small, Window(start = timeAt(-5), end = timeAt(5)), 1 + 2 + 3 + 3)),

      Data(time = timeAt(10), watermark = timeAt(11), (Large, Window(start = timeAt(0), end = timeAt(10)), 10 + 12 + 13)),
      Data(time = timeAt(10), watermark = timeAt(11), (Small, Window(start = timeAt(0), end = timeAt(10)), 1 + 2 + 3 + 3 + 4)),

      Data(time = timeAt(15), watermark = timeAt(15), (Large, Window(start = timeAt(5), end = timeAt(15)), 13 + 11)),
      Data(time = timeAt(15), watermark = timeAt(15), (Small, Window(start = timeAt(5), end = timeAt(15)), 4 + 7)),
      
      Data(time = timeAt(20), watermark = timeAt(20), (Small, Window(start = timeAt(10), end = timeAt(20)), 7)),
      Data(time = timeAt(20), watermark = timeAt(20), (Large, Window(start = timeAt(10), end = timeAt(20)), 11))
    ))
  }

  test("Sliding numbers with late watermarks") {

    val stream = MemoryStream(Vector(
      Data(time = timeAt(1), watermark = timeAt(1), value = 1),
      Data(time = timeAt(2), watermark = timeAt(1), value = 2),
      Data(time = timeAt(3), watermark = timeAt(1), value = 3),
      Data(time = timeAt(4), watermark = timeAt(1), value = 4),
      Data(time = timeAt(5), watermark = timeAt(3), value = 5),
      Data(time = timeAt(6), watermark = timeAt(3), value = 6),
      Data(time = timeAt(7), watermark = timeAt(3), value = 7),
      Data(time = timeAt(8), watermark = timeAt(6), value = 8),
      Data(time = timeAt(9), watermark = timeAt(6), value = 9)
    ))

    val outStream: MemoryStream[List[Int]] = TestPrograms.totalSlidingSums(SlidingWindow(10 seconds, 2 seconds))(stream)

    val actualValues = outStream.vector
    assert(actualValues == Vector(
      Data(time = timeAt(2), watermark = timeAt(3), value = List(1)),
      Data(time = timeAt(4), watermark = timeAt(6), value = List(1, 2, 3)),
      Data(time = timeAt(6), watermark = timeAt(6), value = List(1, 2, 3, 4, 5)),
      Data(time = timeAt(8), watermark = timeAt(8), value = List(1, 2, 3, 4, 5, 6, 7)),
      Data(time = timeAt(10), watermark = timeAt(10), value = List(1, 2, 3, 4, 5, 6, 7, 8, 9)),
      Data(time = timeAt(12), watermark = timeAt(12), value = List(2, 3, 4, 5, 6, 7, 8, 9)),
      Data(time = timeAt(14), watermark = timeAt(14), value = List(4, 5, 6, 7, 8, 9)),
      Data(time = timeAt(16), watermark = timeAt(16), value = List(6, 7, 8, 9)),
      Data(time = timeAt(18), watermark = timeAt(18), value = List(8, 9))
    ))
  }

  private def syncedData[T](time: Instant, value: T): Data[T] = Data(time, time, value)

  private def timeAt(i: Int): Instant = Instant(10000L + i * 1000)

}
