package com.github.flinkalt.programs

import cats.instances.list._
import com.github.flinkalt._
import com.github.flinkalt.memory.DataAndWatermark
import com.github.flinkalt.programs.utils.TestUtils.{at, justBefore, runTestCaseWithFlink, runTestCaseWithMemory}
import com.github.flinkalt.programs.utils.{DStreamFun, TestCase}
import com.github.flinkalt.time._
import com.github.flinkalt.typeinfo.auto._
import org.scalatest.FunSuite

class TotalSlidingSumsTest extends FunSuite {

  import DStream.ops._
  import Windowed.ops._


  // sliding windows with concatenating lists, with a single global state
  def totalSlidingSums[DS[_] : DStream : Windowed](windowType: WindowType)(nums: DS[Int]): DS[List[Int]] = {
    nums
      .map(i => List(i))
      .windowReduce(windowType, _ => ())
  }

  val totalSlidingSumsTestCase = TestCase(
    input = Vector(
      DataAndWatermark(time = at(1 seconds), watermark = at(1 seconds), value = 1),
      DataAndWatermark(time = at(2 seconds), watermark = at(1 seconds), value = 2),
      DataAndWatermark(time = at(3 seconds), watermark = at(1 seconds), value = 3),
      DataAndWatermark(time = at(4 seconds), watermark = at(1 seconds), value = 4),
      DataAndWatermark(time = at(5 seconds), watermark = at(3 seconds), value = 5),
      DataAndWatermark(time = at(6 seconds), watermark = at(3 seconds), value = 6),
      DataAndWatermark(time = at(7 seconds), watermark = at(3 seconds), value = 7),
      DataAndWatermark(time = at(8 seconds), watermark = at(6 seconds), value = 8),
      DataAndWatermark(time = at(9 seconds), watermark = at(6 seconds), value = 9)
    ),
    output = Vector(
      DataAndWatermark(time = justBefore(2 seconds), watermark = at(1 seconds), value = List(1)),
      DataAndWatermark(time = justBefore(4 seconds), watermark = at(3 seconds), value = List(1, 2, 3)),
      DataAndWatermark(time = justBefore(6 seconds), watermark = at(3 seconds), value = List(1, 2, 3, 4, 5)),
      DataAndWatermark(time = justBefore(8 seconds), watermark = at(6 seconds), value = List(1, 2, 3, 4, 5, 6, 7)),
      DataAndWatermark(time = justBefore(10 seconds), watermark = at(6 seconds), value = List(1, 2, 3, 4, 5, 6, 7, 8, 9)),
      DataAndWatermark(time = justBefore(12 seconds), watermark = at(6 seconds), value = List(2, 3, 4, 5, 6, 7, 8, 9)),
      DataAndWatermark(time = justBefore(14 seconds), watermark = at(6 seconds), value = List(4, 5, 6, 7, 8, 9)),
      DataAndWatermark(time = justBefore(16 seconds), watermark = at(6 seconds), value = List(6, 7, 8, 9)),
      DataAndWatermark(time = justBefore(18 seconds), watermark = at(6 seconds), value = List(8, 9))
    ),
    program = new DStreamFun[Int, List[Int]] {
      override def apply[DS[_] : DStream : Windowed : Stateful]: DS[Int] => DS[List[Int]] = {
        totalSlidingSums(SlidingWindow(10 seconds, 2 seconds))
      }
    }
  )

  test("Sliding numbers with late watermarks with Flink") {
    runTestCaseWithFlink(totalSlidingSumsTestCase)
  }

  test("Sliding numbers with late watermarks with Memory") {
    runTestCaseWithMemory(totalSlidingSumsTestCase)
  }
}
