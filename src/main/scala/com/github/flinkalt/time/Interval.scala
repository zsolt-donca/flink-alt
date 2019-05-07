package com.github.flinkalt.time

import cats.instances.either._
import cats.instances.long._
import cats.kernel.Order
import cats.syntax.apply._
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.generic.semiauto
import io.circe.{Decoder, Encoder}

case class Interval(start: Instant, end: Instant) {
  @inline def toDuration: Duration = Duration(end.millis - start.millis)

  def contains(instant: Instant): Boolean = {
    val inst = instant.millis
    start.millis <= inst && inst < end.millis
  }
}

object Interval {
  implicit def intervalTypeInfo: TypeInfo[Interval] = semiauto.deriveTypeInfo

  implicit def intervalEncoder: Encoder[Interval] = io.circe.generic.semiauto.deriveEncoder

  implicit def intervalDecoder: Decoder[Interval] = io.circe.generic.semiauto.deriveDecoder

  def parseZoned(start: String, end: String): Either[String, Interval] = {
    (Instant.parseZoned(start), Instant.parseZoned(end)).mapN(Interval(_, _))
  }

  implicit val intervalOrder: Order[Interval] = Order.whenEqual[Interval](Order.by(_.start.millis), Order.by(_.end.millis))
  implicit val intervalOrdering: Ordering[Interval] = intervalOrder.toOrdering
}
