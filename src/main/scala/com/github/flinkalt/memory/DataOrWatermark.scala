package com.github.flinkalt.memory

import cats.Functor
import com.github.flinkalt.time.Instant

sealed trait DataOrWatermark[+A]
case class JustData[+A](time: Instant, value: A) extends DataOrWatermark[A]
case class JustWatermark(watermark: Instant) extends DataOrWatermark[Nothing]

object DataOrWatermark {
  implicit def memoryElemFunctor: Functor[DataOrWatermark] = new Functor[DataOrWatermark] {
    override def map[A, B](fa: DataOrWatermark[A])(f: A => B): DataOrWatermark[B] = {
      fa match {
        case JustData(time, value) => JustData(time, f(value))
        case JustWatermark(time) => JustWatermark(time)
      }
    }
  }
}

