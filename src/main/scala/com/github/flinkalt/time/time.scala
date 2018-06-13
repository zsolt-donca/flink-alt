package com.github.flinkalt

package object time {

  implicit class IntDurationExt(val value: Int) extends AnyVal {
    @inline def hours: Duration = Duration(value * 60 * 60 * 1000)
    @inline def hour: Duration = Duration(value * 60 * 60 * 1000)

    @inline def minutes: Duration = Duration(value * 60 * 1000)
    @inline def minute: Duration = Duration(value * 60 * 1000)

    @inline def seconds: Duration = Duration(value * 1000)
    @inline def second: Duration = Duration(value * 1000)

    @inline def millis: Duration = Duration(value)
    @inline def milli: Duration = Duration(value)
  }

  implicit class LongTimeExt(val value: Long) extends AnyVal {
    @inline def toInstant: Instant = Instant(value)

    @inline def toDuration: Duration = Duration(value)
  }
}