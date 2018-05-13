package com.github.flinkalt.memory

import cats.Functor
import cats.data.State
import cats.instances.vector._
import cats.syntax.traverse._
import com.github.flinkalt._
import com.github.flinkalt.time.Instant

sealed trait MemoryElem[+A]
case class MemoryData[+A](time: Instant, value: A) extends MemoryElem[A]
case class MemoryWatermark(watermark: Instant) extends MemoryElem[Nothing]

object MemoryElem {
  implicit def memoryElemFunctor: Functor[MemoryElem] = new Functor[MemoryElem] {
    override def map[A, B](fa: MemoryElem[A])(f: A => B): MemoryElem[B] = {
      fa match {
        case MemoryData(time, value) => MemoryData(time, f(value))
        case MemoryWatermark(time) => MemoryWatermark(time)
      }
    }
  }
}

case class MemoryStream[+T](elems: Vector[MemoryElem[T]]) {
  def toData: Vector[Data[T]] = {
    val convertToData: MemoryElem[T] => State[Instant, Vector[Data[T]]] = {
      case MemoryData(time, value) => State(watermark => (watermark, Vector(Data(time, watermark, value))))
      case MemoryWatermark(watermark) => State(_ => (watermark, Vector.empty))
    }

    elems.flatTraverse(convertToData).runA(Instant.minValue).value
  }
}

object MemoryStream {
  implicit def memoryDStream: DStream[MemoryStream] = MemoryDStream
  implicit def memoryStateful: Stateful[MemoryStream] = MemoryStateful
  implicit def memoryWindowed: Windowed[MemoryStream] = MemoryWindowed

  def fromData[T](inputData: Vector[Data[T]]): MemoryStream[T] = {
    val elems = inputData.flatMap {
      case Data(time, watermark, value) => List(MemoryData(time, value), MemoryWatermark(watermark))
    }

    MemoryStream(elems)
  }
}
