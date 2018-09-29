package com.github.flinkalt.memory

import cats.data.State
import cats.instances.map._
import cats.instances.vector._
import cats.kernel.Semigroup
import cats.syntax.semigroup._
import cats.syntax.traverse._
import com.github.flinkalt.api._
import com.github.flinkalt.time._
import com.github.flinkalt.typeinfo.TypeInfo
import org.apache.flink.api.scala.ClosureCleaner

object MemoryWindowed extends Windowed[MemoryStream] {

  override def windowReduce[K: TypeInfo, A: Semigroup : TypeInfo](fa: MemoryStream[A])(windowType: WindowType, key: A => K): MemoryStream[A] = {
    windowReduceMapped(fa)(windowType, key)((_, _, a) => a)
  }

  override def windowReduceMapped[K: TypeInfo, A: Semigroup, B: TypeInfo](fa: MemoryStream[A])(windowType: WindowType, key: A => K)(trigger: WindowMapper[K, A, B]): MemoryStream[B] = {
    ClosureCleaner.ensureSerializable(key)
    ClosureCleaner.ensureSerializable(Some(implicitly[Semigroup[A]]))

    case class ReduceState(lastWatermark: Instant, windows: Map[Window, Map[K, A]])
    val trans: DataOrWatermark[A] => State[ReduceState, Vector[DataOrWatermark[B]]] = {
      case JustData(time, value) =>
        State { case ReduceState(lastWatermark, windows) =>
          val wins = calculateWindows(time, windowType)
          val updatedWindows = wins.map(win => win -> Map(key(value) -> value)).toMap
          val newWindows = windows |+| updatedWindows

          (ReduceState(lastWatermark, newWindows), Vector.empty)
        }

      case JustWatermark(watermark) =>
        State { case ReduceState(lastWatermark, windows) =>
          val (results, remainingWindows) = triggerExpiredWindows(windows, watermark, lastWatermark, trigger)
          (ReduceState(watermark, remainingWindows), results)
        }
    }
    val (lastState, triggered) = fa.elems.flatTraverse(trans).run(ReduceState(Instant.minValue, Map.empty)).value
    val finalResults = triggerWindows(lastState.windows, trigger, lastState.lastWatermark)
    val results = triggered ++ finalResults
    MemoryStream(results)
  }

  private def calculateWindows(time: Instant, windowType: WindowType): Vector[Window] = {
    val (size, slide) = windowType match {
      case WindowTypes.Sliding(winSize, winSlide) => (winSize, winSlide)
      case WindowTypes.Tumbling(winSize) => (winSize, winSize)
    }
    val ratio = size.millis / slide.millis
    val sliceEnd = ceil(time, slide)

    Stream.iterate(Window(sliceEnd - size, sliceEnd))(w => Window(w.start + slide, w.end + slide))
      .take(ratio.toInt)
      .toVector
  }

  private def triggerExpiredWindows[K, A, B](windows: Map[Window, Map[K, A]], watermark: Instant, lastWatermark: Instant, trigger: (K, Window, A) => B): (Vector[DataOrWatermark[B]], Map[Window, Map[K, A]]) = {
    val (triggered, remaining) = windows.partition({ case (w, _) => w.end <= watermark })
    val bs = triggerWindows(triggered, trigger, lastWatermark)
    (bs, remaining)
  }

  private def triggerWindows[B, A, K](triggered: Map[Window, Map[K, A]], trigger: (K, Window, A) => B, watermark: Instant): Vector[DataOrWatermark[B]] = {
    val memData = triggered.flatMap({ case (win, values) => values.map({ case (k, a) => JustData(win.end - (1 milli), trigger(k, win, a)) }) }).toVector
    val memWm = JustWatermark(watermark)
    memWm +: memData.sortBy(_.time.millis)
  }

  private def ceil(time: Instant, size: Duration): Instant = {
    val t = time.millis
    val s = size.millis

    val r = t + (s - t % s)
    Instant(r)
  }
}
