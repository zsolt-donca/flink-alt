package com.github.flinkalt.programs

import cats.instances.int._
import com.github.flinkalt.api.{DStream, Stateful, Windowed, _}
import com.github.flinkalt.memory.DataAndWatermark
import com.github.flinkalt.programs.utils.TestUtils._
import com.github.flinkalt.programs.utils.{DStreamFun, TestCase}
import com.github.flinkalt.time._
import com.github.flinkalt.typeinfo.auto._
import org.scalatest.FunSuite

sealed trait Size
case object Small extends Size
case object Large extends Size

class SlidingSumsBySizeTest extends FunSuite {

  import Windowed.ops._

  def slidingSumsBySize[DS[_] : DStream : Windowed](windowType: WindowType)(nums: DS[Int]): DS[(Size, Window, Int)] = {
    val key: Int => Size = i => if (i < 10) Small else Large
    nums.windowReduceMapped(windowType, key)((size: Size, win: Window, a: Int) => (size, win, a))
  }

  val slidingSumsBySizeTestCase = TestCase(
    input = Vector(
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
    ),
    output = Vector(
      DataAndWatermark(time = justBefore(5 seconds), watermark = at(3 seconds), (Small, Window(start = at(-5 seconds), end = at(5 seconds)), 1 + 2 + 3 + 3)),
      DataAndWatermark(time = justBefore(5 seconds), watermark = at(3 seconds), (Large, Window(start = at(-5 seconds), end = at(5 seconds)), 10 + 12)),

      DataAndWatermark(time = justBefore(10 seconds), watermark = at(7 seconds), (Small, Window(start = at(0 seconds), end = at(10 seconds)), 1 + 2 + 3 + 3 + 4)),
      DataAndWatermark(time = justBefore(10 seconds), watermark = at(7 seconds), (Large, Window(start = at(0 seconds), end = at(10 seconds)), 10 + 12 + 13)),

      DataAndWatermark(time = justBefore(15 seconds), watermark = at(12 seconds), (Large, Window(start = at(5 seconds), end = at(15 seconds)), 13 + 11)),
      DataAndWatermark(time = justBefore(15 seconds), watermark = at(12 seconds), (Small, Window(start = at(5 seconds), end = at(15 seconds)), 4 + 7)),

      DataAndWatermark(time = justBefore(20 seconds), watermark = at(12 seconds), (Large, Window(start = at(10 seconds), end = at(20 seconds)), 11)),
      DataAndWatermark(time = justBefore(20 seconds), watermark = at(12 seconds), (Small, Window(start = at(10 seconds), end = at(20 seconds)), 7))
    ),
    program = new DStreamFun[Int, (Size, Window, Int)] {
      override def apply[DS[_] : DStream : Windowed : Stateful : Processing]: DS[Int] => DS[(Size, Window, Int)] = {
        slidingSumsBySize(WindowTypes.Sliding(10 second, 5 seconds))
      }
    }
  )

  test("Sliding number ladder with Flink") {
    runTestCaseWithFlink(slidingSumsBySizeTestCase)
  }

  test("Sliding number ladder with Memory") {
    runTestCaseWithMemory(slidingSumsBySizeTestCase)
  }
}
