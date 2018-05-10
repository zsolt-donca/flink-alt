package com.github.flinkalt.memory

import cats.Functor
import cats.data.State
import cats.instances.vector._
import cats.instances.map._
import cats.kernel.Semigroup
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.syntax.semigroup._
import com.github.flinkalt.WindowTrigger.WindowTrigger
import com.github.flinkalt.time.{Duration, Instant}
import com.github.flinkalt._

case class Data[+T](time: Instant, watermark: Instant, value: T)
object Data {
  implicit def functor: Functor[Data] = new Functor[Data] {
    override def map[A, B](fa: Data[A])(f: A => B): Data[B] = fa.copy(value = f(fa.value))
  }
}

case class MemoryDStream[+T](vector: Vector[Data[T]])

object MemoryDStream {
  implicit def memoryDStream: DStream[MemoryDStream] = new DStream[MemoryDStream] {

    override def map[A, B](fa: MemoryDStream[A])(f: A => B): MemoryDStream[B] = {
      MemoryDStream(fa.vector.map(data => data.map(f)))
    }

    override def filter[T](f: MemoryDStream[T])(predicate: T => Boolean): MemoryDStream[T] = {
      MemoryDStream(f.vector.filter(data => predicate(data.value)))
    }

    override def flatMap[T, U](f: MemoryDStream[T])(fun: T => Seq[U]): MemoryDStream[U] = {
      MemoryDStream(f.vector.flatMap(data => fun(data.value).map(u => data.as(u))))
    }

    override def collect[T, U](f: MemoryDStream[T])(pf: PartialFunction[T, U]): MemoryDStream[U] = {
      MemoryDStream(f.vector.collect {
        case data if pf.isDefinedAt(data.value) => data.as(pf.apply(data.value))
      })
    }

    override def mapWithState[K, S, A, B](f: MemoryDStream[A])(stateTrans: StateTrans[K, S, A, B]): MemoryDStream[B] = {
      val trans: Data[A] => State[Map[K, S], Data[B]] = da => stateByKey(stateTrans.key(da.value), stateTrans.trans(da.value)).map(da.as)
      val vector = f.vector.traverse(trans).runA(Map.empty).value
      MemoryDStream(vector)
    }

    override def flatMapWithState[K, S, A, B](f: MemoryDStream[A])(stateTrans: StateTrans[K, S, A, Vector[B]]): MemoryDStream[B] = {
      val trans: Data[A] => State[Map[K, S], Vector[Data[B]]] = da => stateByKey(stateTrans.key(da.value), stateTrans.trans(da.value)).map(v => v.map(da.as))
      val vector = f.vector.flatTraverse(trans).runA(Map.empty).value
      MemoryDStream(vector)
    }

    override def windowReduce[K, A: Semigroup, B](fa: MemoryDStream[A])(windowType: WindowType, key: A => K)(trigger: WindowTrigger[K, A, B]): MemoryDStream[B] = {
      val trans: Data[A] => State[Map[Window, Map[K, A]], Vector[Data[B]]] = da => {
        State { windows =>
          val wins = calculateWindows(da.time, windowType)
          val k = key(da.value)
          val updatedWindows = wins.map(win => win -> Map(k -> da.value)).toMap
          val newWindows = windows |+| updatedWindows
          val (results, remainingWindows) = triggerExpiredWindows(newWindows, da.watermark, trigger)

          (remainingWindows, results)
        }
      }
      val (winState, vector) = fa.vector.flatTraverse(trans).run(Map.empty).value
      val finalResults = triggerWindows(winState, trigger)
      MemoryDStream(vector ++ finalResults)
    }
  }

  private def stateByKey[K, S, A](key: K, st: State[Option[S], A]): State[Map[K, S], A] = {
    val read: Map[K, S] => Option[S] = map => map.get(key)
    val write: (Map[K, S], Option[S]) => Map[K, S] = (map, os) => os.map(s => map.updated(key, s)).getOrElse(map)
    st.transformS(read, write)
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
    val bs = triggerWindows(triggered, trigger)
    (bs, remaining)
  }

  private def triggerWindows[B, A, K](triggered: Map[Window, Map[K, A]], trigger: (K, Window, A) => B) = {
    val bs = triggered.flatMap({ case (win, values) => values.map({ case (k, a) => Data(win.end, win.end, trigger(k, win, a)) }) }).toVector
    bs
  }

  private def ceil(time: Instant, size: Duration): Instant = {
    val t = time.millis
    val s = size.millis

    val r = t + (s - t % s)
    Instant(r)
  }
}