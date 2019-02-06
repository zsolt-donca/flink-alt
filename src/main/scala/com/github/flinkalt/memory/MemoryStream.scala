package com.github.flinkalt.memory

import cats.data.State
import cats.instances.vector._
import cats.syntax.traverse._
import com.github.flinkalt.api.{DStream, Processing, Stateful, Windowed}
import com.github.flinkalt.time.Instant
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.util.InstantiationUtil

case class MemoryStream[+T](elems: Vector[DataOrWatermark[T]]) {
  def toPostData: Vector[DataAndWatermark[T]] = {
    val convertToData: DataOrWatermark[T] => State[Instant, Vector[DataAndWatermark[T]]] = {
      case JustData(time, value) => State(watermark => (watermark, Vector(DataAndWatermark(time, watermark, value))))
      case JustWatermark(watermark) => State(_ => (watermark, Vector.empty))
    }

    elems.flatTraverse(convertToData).runA(Instant.minValue).value
  }

  def toPreData: Vector[DataAndWatermark[T]] = {
    val convertToData: DataOrWatermark[T] => State[Vector[JustData[T]], Vector[DataAndWatermark[T]]] = {
      case jd@JustData(_, _) => State(elems => (elems :+ jd, Vector.empty))
      case JustWatermark(watermark) => State(elems => (Vector.empty, elems.map(elem => DataAndWatermark(elem.time, watermark, elem.value))))
    }

    elems.flatTraverse(convertToData).runA(Vector.empty).value
  }

  def collectValues: Vector[T] = {
    elems.collect {
      case JustData(_, t) => t
    }
  }
}

object MemoryStream {
  implicit def memoryDStream: DStream[MemoryStream] = MemoryDStream

  implicit def memoryStateful: Stateful[MemoryStream] = MemoryStateful

  implicit def memoryWindowed: Windowed[MemoryStream] = MemoryWindowed

  implicit def memoryProcessing: Processing[MemoryStream] = MemoryProcessing

  def empty[T]: MemoryStream[T] = MemoryStream(Vector.empty)

  def fromData[T](inputData: Vector[DataAndWatermark[T]]): MemoryStream[T] = {
    val elems = inputData.flatMap {
      case DataAndWatermark(time, watermark, value) => List(JustData(time, value), JustWatermark(watermark))
    }

    MemoryStream(elems)
  }

  def ensureSerializable(func: AnyRef): Unit = {
    try {
      InstantiationUtil.serializeObject(func)
    } catch {
      case ex: Exception => throw new InvalidProgramException("Task not serializable", ex)
    }
  }
}
