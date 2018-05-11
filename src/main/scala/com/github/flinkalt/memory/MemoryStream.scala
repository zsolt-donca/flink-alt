package com.github.flinkalt.memory

import cats.Functor
import com.github.flinkalt._
import com.github.flinkalt.time.Instant

case class Data[+T](time: Instant, watermark: Instant, value: T)

object Data {
  implicit def functor: Functor[Data] = new Functor[Data] {
    override def map[A, B](fa: Data[A])(f: A => B): Data[B] = fa.copy(value = f(fa.value))
  }
}

case class MemoryStream[+T](vector: Vector[Data[T]])

object MemoryStream {
  implicit def memoryDStream: DStream[MemoryStream] = MemoryDStream
  implicit def memoryStateful: Stateful[MemoryStream] = MemoryStateful
  implicit def memoryWindowed: Windowed[MemoryStream] = MemoryWindowed
}