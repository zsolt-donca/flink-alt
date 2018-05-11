package com.github.flinkalt.time

import cats.Order
import cats.instances.long._

case class Duration(millis: Long) extends AnyVal

object Duration {
  implicit def durationOrder: Order[Duration] = Order.by(_.millis)
}
