package com.github.flinkalt.memory

import cats.Order
import cats.syntax.functor._
import com.github.flinkalt.DStream
import com.github.flinkalt.time.Instant
import com.github.flinkalt.typeinfo.TypeInfo
import org.apache.flink.api.scala.ClosureCleaner

object MemoryDStream extends DStream[MemoryStream] {
  override def map[A, B: TypeInfo](fa: MemoryStream[A])(f: A => B): MemoryStream[B] = {
    ClosureCleaner.ensureSerializable(f)
    fa.copy(elems = fa.elems.map(data => data.map(f)))
  }

  override def filter[T](f: MemoryStream[T])(predicate: T => Boolean): MemoryStream[T] = {
    ClosureCleaner.ensureSerializable(predicate)
    f.copy(elems = f.elems.filter {
      case JustData(_, value) => predicate(value)
      case JustWatermark(_) => true
    })
  }

  override def flatMap[T, U: TypeInfo](f: MemoryStream[T])(fun: T => Seq[U]): MemoryStream[U] = {
    ClosureCleaner.ensureSerializable(fun)
    f.copy(elems = f.elems.flatMap {
      case JustData(time, value) => fun(value).map(u => JustData(time, u))
      case JustWatermark(time) => List(JustWatermark(time))
    })
  }

  override def collect[T, U: TypeInfo](f: MemoryStream[T])(pf: PartialFunction[T, U]): MemoryStream[U] = {
    ClosureCleaner.ensureSerializable(pf)
    flatMap(f)(t => pf.lift(t).toList)
  }

  override def union[A](f: MemoryStream[A])(g: MemoryStream[A]): MemoryStream[A] = {
    val sortedData = (f.toPreData ++ g.toPreData).sorted
    MemoryStream.fromData(sortedData)
  }

  private implicit def dataOrder[T]: Order[DataAndWatermark[T]] = Order.whenEqual(Order.by[DataAndWatermark[T], Instant](_.watermark), Order.by[DataAndWatermark[T], Instant](_.time))

  private implicit def toOrdering[T: Order]: Ordering[T] = Order[T].toOrdering
}
