package com.github.flinkalt.typeinfo

import com.github.flinkalt.Data
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.scalatest.FunSuite

class TypeInfoTest extends FunSuite {
  case class Envelope[T](s: String, i: Int, contents: T)

  sealed trait Tree[A]
  case class Node[A](left: Tree[A], right: Tree[A]) extends Tree[A]
  case class Leaf[A](value: A) extends Tree[A]

  sealed trait Choice
  case object Yes extends Choice
  case object No extends Choice
  case class Other(reason: String) extends Choice

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

  test("Type information equality of Either") {
    def t1 = implicitly[TypeInformation[Either[String, Int]]]
    def t2 = implicitly[TypeInformation[Either[String, Int]]]

    assert(!(t1 eq t2))
    assert(t1 == t2)
  }

  test("Type information equality of Option") {
    def t1 = implicitly[TypeInformation[Option[String]]]
    def t2 = implicitly[TypeInformation[Option[String]]]

    assert(!(t1 eq t2))
    assert(t1 == t2)
  }

  test("Type information inequality of Option") {
    def t1 = implicitly[TypeInformation[Option[Double]]]
    def t2 = implicitly[TypeInformation[Option[Int]]]

    assert(!(t1 eq t2))
    assert(t1 != t2)
  }

  test("Type information equality for Int") {
    def t1 = implicitly[TypeInformation[Int]]
    def t2 = implicitly[TypeInformation[Int]]

    assert(t1 == t2)
  }

  test("Type information equality of product") {
    def t1 = implicitly[TypeInformation[Envelope[(String, Int)]]]
    def t2 = implicitly[TypeInformation[Envelope[(String, Int)]]]

    assert(!(t1 eq t2))
    assert(t1 == t2)
  }

  test("Type information equality of non-recursive coproduct") {
    def t1 = implicitly[TypeInformation[Choice]]
    def t2 = implicitly[TypeInformation[Choice]]

    assert(!(t1 eq t2))
    assert(t1 == t2)
  }

  // TODO this is a known limitation at the moment
  // TODO this triggers a StackOverflowError as the typeclass instances themselves are recursive - need to find a way to make it work
  test("Type information equality of recursive coproduct") {
    def t1 = implicitly[TypeInformation[Tree[Int]]]
    def t2 = implicitly[TypeInformation[Tree[Int]]]

    assert(!(t1 eq t2))
    assert(t1 == t2)
  }
}
