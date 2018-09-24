package com.github.flinkalt.memory

import cats.syntax.functor._
import com.github.flinkalt.DStream
import com.github.flinkalt.typeinfo.TypeInfo

object MemoryDStream extends DStream[MemoryStream] {
  override def map[A, B: TypeInfo](fa: MemoryStream[A])(f: A => B): MemoryStream[B] = {
    fa.copy(elems = fa.elems.map(data => data.map(f)))
  }

  override def filter[T](f: MemoryStream[T])(predicate: T => Boolean): MemoryStream[T] = {
    f.copy(elems = f.elems.filter {
      case JustData(_, value) => predicate(value)
      case JustWatermark(_) => true
    })
  }

  override def flatMap[T, U: TypeInfo](f: MemoryStream[T])(fun: T => Seq[U]): MemoryStream[U] = {
    f.copy(elems = f.elems.flatMap {
      case JustData(time, value) => fun(value).map(u => JustData(time, u))
      case JustWatermark(time) => List(JustWatermark(time))
    })
  }

  override def collect[T, U: TypeInfo](f: MemoryStream[T])(pf: PartialFunction[T, U]): MemoryStream[U] = {
    flatMap(f)(t => pf.lift(t).toList)
  }
}
