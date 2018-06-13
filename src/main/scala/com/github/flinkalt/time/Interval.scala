package com.github.flinkalt.time

import cats.instances.long._
import cats.kernel.Order

case class Interval(start: Instant, end: Instant) {
  @inline def toDuration: Duration = Duration(end.millis - start.millis)

  def contains(instant: Instant): Boolean = {
    val inst = instant.millis
    start.millis <= inst && inst < end.millis
  }
}

object Interval {
  def parseZoned(start: String, end: String): Interval = {
    Interval(Instant.parseZoned(start), Instant.parseZoned(end))
  }

  implicit val intervalOrder: Order[Interval] = Order.whenEqual[Interval](Order.by(_.start.millis), Order.by(_.end.millis))
  implicit val intervalOrdering: Ordering[Interval] = intervalOrder.toOrdering
}
