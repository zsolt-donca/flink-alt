package com.github.flinkalt.time

import cats.syntax.either._
import org.scalatest.FunSuite

class InstantTest extends FunSuite {
  test("Multiple of a minute") {
    val inst = Instant(123456L * 60 * 1000)
    val str = inst.toString

    val parsedToJavaInst =
      Either.catchOnly[RuntimeException](java.time.Instant.parse(str))
    assert(parsedToJavaInst.map(_.toEpochMilli) == Right(inst.millis))

    val parsedInst = parsedToJavaInst.flatMap(
      javaInst => Instant.parseInstant(javaInst.toString)
    )
    assert(parsedInst == Right(inst))
  }
}
