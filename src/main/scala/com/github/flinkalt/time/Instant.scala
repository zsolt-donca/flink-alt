package com.github.flinkalt.time

import java.time.ZoneOffset
import java.time.format.DateTimeParseException

import cats.Order
import cats.instances.long._
import cats.syntax.either._
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.generic.semiauto
import io.circe._

case class Instant(millis: Long) extends AnyVal {
  @inline def -(duration: Duration): Instant = Instant(millis - duration.millis)
  @inline def +(duration: Duration): Instant = Instant(millis + duration.millis)

  @inline def >(that: Instant): Boolean = this.millis > that.millis
  @inline def >=(that: Instant): Boolean = this.millis >= that.millis
  @inline def <(that: Instant): Boolean = this.millis < that.millis
  @inline def <=(that: Instant): Boolean = this.millis <= that.millis

  @inline def durationBetween(end: Instant): Duration = Duration(end.millis - this.millis)

  override def toString: String = {
    java.time.ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(millis), ZoneOffset.UTC).toString
  }
}

object Instant {
  implicit def instantTypeInfo: TypeInfo[Instant] = semiauto.deriveTypeInfo

  implicit def instantEncoder: Encoder[Instant] = (a: Instant) => Json.fromString(a.toString)

  implicit def instantDecoder: Decoder[Instant] = (c: HCursor) => {
    val asZoned = Decoder.decodeString(c)
      .flatMap(s => parseZoned(s).leftMap(e => DecodingFailure(e, List.empty)))

    lazy val asLong = Decoder.decodeLong(c).map(long => Instant(long))

    asZoned orElse asLong
  }

  val minValue: Instant = Instant(Long.MinValue)

  implicit def instantOrder: Order[Instant] = Order.by(_.millis)
  implicit def instantOrdering: Ordering[Instant] = Ordering.by(_.millis)

  def min(left: Instant, right: Instant): Instant = {
    if (left.millis <= right.millis)
      left
    else
      right
  }

  def max(left: Instant, right: Instant): Instant = {
    if (left.millis >= right.millis)
      left
    else
      right
  }

  def now(): Instant = {
    val millis = java.time.ZonedDateTime.now(ZoneOffset.UTC).toInstant.toEpochMilli
    Instant(millis)
  }

  def unsafeParseZoned(str: String): Instant = {
    val millis = java.time.ZonedDateTime.parse(str).toInstant.toEpochMilli
    Instant(millis)
  }

  def parseZoned(str: String): Either[String, Instant] = {
    Either.catchOnly[DateTimeParseException](unsafeParseZoned(str))
      .leftMap(e => e.getMessage)
  }
}
