package com.github.flinkalt.time

import cats.Order
import cats.instances.long._

case class Duration(millis: Long) extends AnyVal

object Duration {
  implicit def durationOrder: Order[Duration] = Order.by(_.millis)
}

case class Instant(millis: Long) extends AnyVal {
  @inline def -(duration: Duration): Instant = Instant(millis - duration.millis)
  @inline def +(duration: Duration): Instant = Instant(millis + duration.millis)

  @inline def >(that: Instant): Boolean = this.millis > that.millis
  @inline def >=(that: Instant): Boolean = this.millis >= that.millis
  @inline def <(that: Instant): Boolean = this.millis < that.millis
  @inline def <=(that: Instant): Boolean = this.millis <= that.millis
}

object Instant {
  implicit def instantOrder: Order[Instant] = Order.by(_.millis)
}