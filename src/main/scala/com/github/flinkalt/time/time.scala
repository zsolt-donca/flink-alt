package com.github.flinkalt

package object time {

  implicit class LongDurationExt(val value: Long) extends AnyVal {
    @inline def weeks: Duration = Duration(value * 60L * 60L * 1000L * 24L * 7L)
    @inline def week: Duration = Duration(value * 60L * 60L * 1000L * 24L * 7L)

    @inline def days: Duration = Duration(value * 60L * 60L * 1000L * 24L)
    @inline def day: Duration = Duration(value * 60L * 60L * 1000L * 24L)

    @inline def hours: Duration = Duration(value * 60L * 60L * 1000L)
    @inline def hour: Duration = Duration(value * 60L * 60L * 1000L)

    @inline def minutes: Duration = Duration(value * 60L * 1000L)
    @inline def minute: Duration = Duration(value * 60L * 1000L)

    @inline def seconds: Duration = Duration(value * 1000L)
    @inline def second: Duration = Duration(value * 1000L)

    @inline def millis: Duration = Duration(value)
    @inline def milli: Duration = Duration(value)
  }

  implicit class IntDurationExt(val value: Int) extends AnyVal {
    @inline def weeks: Duration = Duration(value * 60L * 60L * 1000L * 24L * 7L)
    @inline def week: Duration = Duration(value * 60L * 60L * 1000L * 24L * 7L)

    @inline def days: Duration = Duration(value * 60L * 60L * 1000L * 24L)
    @inline def day: Duration = Duration(value * 60L * 60L * 1000L * 24L)

    @inline def hours: Duration = Duration(value * 60L * 60L * 1000L)
    @inline def hour: Duration = Duration(value * 60L * 60L * 1000L)

    @inline def minutes: Duration = Duration(value * 60L * 1000L)
    @inline def minute: Duration = Duration(value * 60L * 1000L)

    @inline def seconds: Duration = Duration(value * 1000L)
    @inline def second: Duration = Duration(value * 1000L)

    @inline def millis: Duration = Duration(value)
    @inline def milli: Duration = Duration(value)
  }

  implicit class LongTimeExt(val value: Long) extends AnyVal {
    @inline def toInstant: Instant = Instant(value)

    @inline def toDuration: Duration = Duration(value)
  }
}