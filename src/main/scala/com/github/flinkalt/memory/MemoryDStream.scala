package com.github.flinkalt.memory

import cats.syntax.functor._
import com.github.flinkalt.{DStream, TypeInfo}

object MemoryDStream extends DStream[MemoryStream] {
  override def map[A, B: TypeInfo](fa: MemoryStream[A])(f: A => B): MemoryStream[B] = {
    fa.copy(elems = fa.elems.map(data => data.map(f)))
  }

  override def filter[T](f: MemoryStream[T])(predicate: T => Boolean): MemoryStream[T] = {
    f.copy(elems = f.elems.filter {
      case MemoryData(_, value) => predicate(value)
      case MemoryWatermark(_) => true
    })
  }

  override def flatMap[T, U: TypeInfo](f: MemoryStream[T])(fun: T => Seq[U]): MemoryStream[U] = {
    f.copy(elems = f.elems.flatMap {
      case MemoryData(time, value) => fun(value).map(u => MemoryData(time, u))
      case MemoryWatermark(time) => List(MemoryWatermark(time))
    })
  }

  override def collect[T, U: TypeInfo](f: MemoryStream[T])(pf: PartialFunction[T, U]): MemoryStream[U] = {
    flatMap(f)(t => pf.lift(t).toList)
  }
}
