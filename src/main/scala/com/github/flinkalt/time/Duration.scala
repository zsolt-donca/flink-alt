package com.github.flinkalt.time

import cats.Order
import cats.instances.list._
import cats.instances.long._
import cats.instances.option._
import cats.kernel.CommutativeMonoid
import cats.syntax.either._
import cats.syntax.foldable._
import cats.syntax.traverse._
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.generic.semiauto
import io.circe._

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
  implicit def durationTypeInfo: TypeInfo[Duration] = semiauto.deriveTypeInfo

  implicit val durationEncoder: Encoder[Duration] = (a: Duration) => Json.fromLong(a.millis)

  implicit val durationDecoder: Decoder[Duration] = (c: HCursor) => {
    val decodedStr = Decoder.decodeString(c).orElse(Decoder.decodeLong(c).map(_.toString))
    decodedStr.flatMap(str => parse(str).toRight(DecodingFailure(s"Decoding duration: $str", List.empty)))
  }

  implicit def durationKeyEncoder: KeyEncoder[Duration] = (a: Duration) => a.millis.toString

  implicit def durationKeyDecoder: KeyDecoder[Duration] = (d: String) => parse(d)

  def parse(s: String): Option[Duration] = {
    val matches = valueAndUnit.findAllIn(s).toList
    val durations = matches.traverse(parseToDurationPF.lift)
    durations.map(_.combineAll)
  }

  val zero: Duration = Duration(0L)

  implicit def durationOrder: Order[Duration] = Order.by(_.millis)
  implicit def durationOrdering: Ordering[Duration] = Ordering.by(_.millis)

  implicit def durationCommutativeMonoid: CommutativeMonoid[Duration] = new CommutativeMonoid[Duration] {
    override def empty: Duration = zero

    override def combine(x: Duration, y: Duration): Duration = x + y
  }

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

  private val valueAndUnit = "(\\d+)\\s*(\\w*)".r

  private val parseToDurationPF: PartialFunction[String, Duration] = {
    case valueAndUnit(LongValue(i), "")                                          => i millis
    case valueAndUnit(LongValue(i), "ms" | "millis" | "milliseconds")            => i millis
    case valueAndUnit(LongValue(i), "s" | "sec" | "secs" | "second" | "seconds") => i seconds
    case valueAndUnit(LongValue(i), "m" | "minute" | "minutes")                  => i minutes
    case valueAndUnit(LongValue(i), "h" | "hour" | "hours")                      => i hours
    case valueAndUnit(LongValue(i), "d" | "day" | "days")                        => (24 * i) hours
    case valueAndUnit(LongValue(i), "w" | "week" | "weeks")                      => (7 * 24 * i) hours
  }

  private object LongValue {
    def unapply(str: String): Option[Long] = {
      Either.catchOnly[NumberFormatException](str.toLong).toOption
    }
  }

}
