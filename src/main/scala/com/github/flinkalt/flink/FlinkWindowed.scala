package com.github.flinkalt.flink

import cats.kernel.Semigroup
import com.github.flinkalt.api._
import com.github.flinkalt.time.Instant
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkWindowed extends Windowed[DataStream] {

  type TimeWindowAssigner = WindowAssigner[Any, TimeWindow]

  override def windowReduce[K: TypeInfo, A: Semigroup : TypeInfo](dataStream: DataStream[A])(windowType: WindowType, key: A => K): DataStream[A] = {
    val windowAssigner = timeWindowAssigner(windowType)
    val combiner: (A, A) => A = Semigroup[A].combine

    dataStream
      .keyBy(key)
      .window(windowAssigner)
      .reduce(combiner)
  }

  override def windowReduceMapped[K: TypeInfo, A: Semigroup, B: TypeInfo](dataStream: DataStream[A])(windowType: WindowType, key: A => K)(trigger: WindowMapper[K, A, B]): DataStream[B] = {
    val windowAssigner = timeWindowAssigner(windowType)
    val combiner: (A, A) => A = Semigroup[A].combine
    val windowFunction = (k: K, win: TimeWindow, as: Iterable[A], collector: Collector[B]) => {
      val window = Window(Instant(win.getStart), Instant(win.getEnd))
      val a = as.reduce(combiner)
      val b = trigger(k, window, a)
      collector.collect(b)
    }

    dataStream
      .keyBy(key)
      .window(windowAssigner)
      .reduce[B](combiner, windowFunction)
  }

  private def timeWindowAssigner[B, A: Semigroup, K](windowType: WindowType): TimeWindowAssigner = {
    windowType match {
      case WindowTypes.Sliding(size, slide) => SlidingEventTimeWindows.of(Time.milliseconds(size.millis), Time.milliseconds(slide.millis)).asInstanceOf[TimeWindowAssigner]
      case WindowTypes.Tumbling(size) => TumblingEventTimeWindows.of(Time.milliseconds(size.millis)).asInstanceOf[TimeWindowAssigner]
    }
  }
}
