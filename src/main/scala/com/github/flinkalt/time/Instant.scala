package com.github.flinkalt.time

import java.time.ZoneOffset

import cats.Order
import cats.instances.long._

case class Instant(millis: Long) extends AnyVal {
  @inline def -(duration: Duration): Instant = Instant(millis - duration.millis)
  @inline def +(duration: Duration): Instant = Instant(millis + duration.millis)

  @inline def >(that: Instant): Boolean = this.millis > that.millis
  @inline def >=(that: Instant): Boolean = this.millis >= that.millis
  @inline def <(that: Instant): Boolean = this.millis < that.millis
  @inline def <=(that: Instant): Boolean = this.millis <= that.millis

  @inline def durationBetween(end: Instant): Duration = Duration(end.millis - this.millis)

  override def toString: String = {
    java.time.LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(millis), ZoneOffset.UTC).toString
  }
}

object Instant {
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

  def parseZoned(str: String): Instant = {
    val millis = java.time.ZonedDateTime.parse(str).toInstant.toEpochMilli
    Instant(millis)
  }
}
