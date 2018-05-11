package com.github.flinkalt.memory

import cats.syntax.functor._
import com.github.flinkalt.DStream

object MemoryDStream extends DStream[MemoryStream] {
  override def map[A, B](fa: MemoryStream[A])(f: A => B): MemoryStream[B] = {
    MemoryStream(fa.vector.map(data => data.map(f)))
  }

  override def filter[T](f: MemoryStream[T])(predicate: T => Boolean): MemoryStream[T] = {
    MemoryStream(f.vector.filter(data => predicate(data.value)))
  }

  override def flatMap[T, U](f: MemoryStream[T])(fun: T => Seq[U]): MemoryStream[U] = {
    MemoryStream(f.vector.flatMap(data => fun(data.value).map(u => data.as(u))))
  }

  override def collect[T, U](f: MemoryStream[T])(pf: PartialFunction[T, U]): MemoryStream[U] = {
    MemoryStream(f.vector.collect {
      case data if pf.isDefinedAt(data.value) => data.as(pf.apply(data.value))
    })
  }
}
