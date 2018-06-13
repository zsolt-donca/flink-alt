package com.github.flinkalt.time

import cats.Order
import cats.instances.long._

case class Duration(millis: Long) extends AnyVal {
  @inline def +(duration: Duration): Duration = Duration(millis + duration.millis)
  @inline def -(duration: Duration): Duration = Duration(millis - duration.millis)

  @inline def >(that: Duration): Boolean = this.millis > that.millis
  @inline def >=(that: Duration): Boolean = this.millis >= that.millis
  @inline def <(that: Duration): Boolean = this.millis < that.millis
  @inline def <=(that: Duration): Boolean = this.millis <= that.millis

  override def toString: String = java.time.Duration.ofMillis(millis).toString
}

object Duration {
  val zero: Duration = Duration(0L)

  implicit def durationOrder: Order[Duration] = Order.by(_.millis)
  implicit def durationOrdering: Ordering[Duration] = Ordering.by(_.millis)

  def min(left: Duration, right: Duration): Duration = {
    if (left.millis <= right.millis)
      left
    else
      right
  }

  def max(left: Duration, right: Duration): Duration = {
    if (left.millis >= right.millis)
      left
    else
      right
  }
}
