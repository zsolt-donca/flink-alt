package com.github.flinkalt.time

import org.scalatest.FunSuite

class DurationTest extends FunSuite {
  test("Test with zero") {
    val actual = Duration.parse("0")
    val expected = Some(Duration(0))
    assert(actual == expected)
  }

  test("Test with 1 ms") {
    val actual = Duration.parse("1 ms")
    val expected = Some(1 millis)
    assert(actual == expected)
  }

  test("Test with 2 sec") {
    val actual = Duration.parse("2 sec")
    val expected = Some(2 seconds)
    assert(actual == expected)
  }

  test("Test with 1 hour and 2 minutes and 3 seconds and 4 millis") {
    val actual = Duration.parse("1 hour 2 minutes 3 seconds 4 millis")
    val expected = Some(1.hour + 2.minutes + 3.seconds + 4.millis)
    assert(actual == expected)
  }

}
