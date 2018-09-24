package com.github.flinkalt.memory

import cats.Functor
import com.github.flinkalt.time.Instant

case class DataAndWatermark[+T](time: Instant, watermark: Instant, value: T)

object DataAndWatermark {
  implicit def functor: Functor[DataAndWatermark] = new Functor[DataAndWatermark] {
    override def map[A, B](fa: DataAndWatermark[A])(f: A => B): DataAndWatermark[B] = fa.copy(value = f(fa.value))
  }
}