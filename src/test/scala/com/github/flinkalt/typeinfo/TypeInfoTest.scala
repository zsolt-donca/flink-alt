package com.github.flinkalt.typeinfo

import org.scalatest.FunSuite

class TypeInfoTest extends FunSuite {
  case class Envelope[T](s: String, i: Int, contents: T)

  test("Type info various types") {
    TypeInfo[String]
    TypeInfo[Int]
    TypeInfo[Long]
    TypeInfo[(String, Long, Int)]

    def envelopeTypeInfo[T: TypeInfo]: TypeInfo[Envelope[T]] = TypeInfo[Envelope[T]]

    envelopeTypeInfo[String]
  }
}
