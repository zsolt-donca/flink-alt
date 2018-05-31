package com.github.flinkalt.typeinfo

import com.github.flinkalt.Data
import org.scalatest.FunSuite

class TypeInfoTest extends FunSuite {
  case class Envelope[T](s: String, i: Int, contents: T)

  sealed trait Tree[A]
  case class Node[A](left: Tree[A], right: Tree[A]) extends Tree[A]
  case class Leaf[A](value: A) extends Tree[A]

  test("Type info various types") {
    TypeInfo[String]
    TypeInfo[Int]
    TypeInfo[Long]
  }

  test("Type info of ADTs") {
    TypeInfo[(String, Long, Int)]

    TypeInfo[Data[String]]
    TypeInfo[Envelope[Data[String]]]

    TypeInfo[Tree[String]]
  }

  test("Parameterized type info") {
    def envelopeTypeInfo[T: TypeInfo]: TypeInfo[Envelope[T]] = TypeInfo[Envelope[T]]
    envelopeTypeInfo[String]

    def dataTypeInfo[T: TypeInfo]: TypeInfo[Data[T]] = TypeInfo[Data[T]]
    dataTypeInfo[String]
  }
}
