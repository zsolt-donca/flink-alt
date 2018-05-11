package com.github.flinkalt.memory

import cats.data.State
import cats.instances.map._
import cats.instances.vector._
import cats.kernel.{Order, Semigroup}
import cats.syntax.semigroup._
import cats.syntax.traverse._
import com.github.flinkalt.Windowed.WindowMapper
import com.github.flinkalt._
import com.github.flinkalt.time.{Duration, Instant}

object MemoryWindowed extends Windowed[MemoryStream] {

  override def windowReduce[K: TypeInfo, A: Semigroup : TypeInfo](fa: MemoryStream[A])(windowType: WindowType, key: A => K): MemoryStream[A] = {
    windowReduceMapped(fa)(windowType, key)((_, _, a) => a)
  }

  override def windowReduceMapped[K: TypeInfo, A: Semigroup, B: TypeInfo](fa: MemoryStream[A])(windowType: WindowType, key: A => K)(trigger: WindowMapper[K, A, B]): MemoryStream[B] = {
    case class ReduceState(lastWatermark: Option[Instant], windows: Map[Window, Map[K, A]])
    val trans: Data[A] => State[ReduceState, Vector[Data[B]]] = da => {
      State { case ReduceState(lastWatermark, windows) =>
        lastWatermark.filter(last => last > da.watermark).foreach(last => sys.error(s"Watermark moves backwards; last: $last, current: ${da.watermark}"))

        val wins = calculateWindows(da.time, windowType)
        val k = key(da.value)
        val updatedWindows = wins.map(win => win -> Map(k -> da.value)).toMap
        val newWindows = windows |+| updatedWindows
        val (results, remainingWindows) = triggerExpiredWindows(newWindows, da.watermark, trigger)

        (ReduceState(Some(da.watermark), remainingWindows), results)
      }
    }
    val (lastState, triggered) = fa.vector.flatTraverse(trans).run(ReduceState(None, Map.empty)).value
    val finalResults = lastState.lastWatermark.map(watermark => triggerWindows(lastState.windows, trigger, watermark)).getOrElse(Vector.empty)
    val results = triggered ++ finalResults
    MemoryStream(results)
  }

  private def calculateWindows(time: Instant, windowType: WindowType): Vector[Window] = {
    val (size, slide) = windowType match {
      case SlidingWindow(winSize, winSlide) => (winSize, winSlide)
      case TumblingWindow(winSize) => (winSize, winSize)
    }
    val ratio = size.millis / slide.millis
    val sliceEnd = ceil(time, slide)

    Stream.iterate(Window(sliceEnd - size, sliceEnd))(w => Window(w.start + slide, w.end + slide))
      .take(ratio.toInt)
      .toVector
  }

  private def triggerExpiredWindows[K, A, B](windows: Map[Window, Map[K, A]], watermark: Instant, trigger: (K, Window, A) => B): (Vector[Data[B]], Map[Window, Map[K, A]]) = {
    val (triggered, remaining) = windows.partition({ case (w, _) => w.end <= watermark })
    val bs = triggerWindows(triggered, trigger, watermark)
    (bs, remaining)
  }

  private def triggerWindows[B, A, K](triggered: Map[Window, Map[K, A]], trigger: (K, Window, A) => B, watermark: Instant): Vector[Data[B]] = {
    val results = triggered.flatMap({ case (win, values) => values.map({ case (k, a) => Data(win.end, Order.max(watermark, win.end), trigger(k, win, a)) }) }).toVector
    results.sortBy(_.time.millis)
  }

  private def ceil(time: Instant, size: Duration): Instant = {
    val t = time.millis
    val s = size.millis

    val r = t + (s - t % s)
    Instant(r)
  }
}
