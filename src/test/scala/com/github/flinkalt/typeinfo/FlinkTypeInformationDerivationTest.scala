package com.github.flinkalt.typeinfo

import cats.data.ValidatedNel
import com.github.flinkalt.memory.DataAndWatermark
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.scalatest.FunSuite

class FlinkTypeInformationDerivationTest extends FunSuite {
  case class Envelope[T](s: String, i: Int, contents: T)

  sealed trait Tree[A]
  case class Node[A](left: Tree[A], right: Tree[A]) extends Tree[A]
  case class Leaf[A](value: A) extends Tree[A]

  sealed trait Choice
  case object Yes extends Choice
  case object No extends Choice
  case class Other(reason: String) extends Choice

  sealed trait ErrorType
  case class SomeError(reason: String) extends ErrorType
  case object SomeUnknownError extends ErrorType
  case class SomeException(ex: Throwable) extends ErrorType

  implicit def throwableToByteArrayInjection: Injection[Throwable, Array[Byte]] = Injection(
    _ => sys.error("The implementation is irrelevant"),
    _ => sys.error("The implementation is irrelevant")
  )

  test("Type info various types") {
    implicitly[TypeInformation[String]]
    implicitly[TypeInformation[Int]]
    implicitly[TypeInformation[Long]]
  }

  test("Type info of ADTs") {
    implicitly[TypeInformation[(String, Long, Int)]]

    implicitly[TypeInformation[DataAndWatermark[String]]]
    implicitly[TypeInformation[Envelope[DataAndWatermark[String]]]]

    implicitly[TypeInformation[Tree[String]]]
  }

  test("Parameterized type info") {
    def envelopeTypeInfo[T: TypeInfo]: TypeInfo[Envelope[T]] = TypeInfo[Envelope[T]]
    envelopeTypeInfo[String]
    envelopeTypeInfo[(String, Int, Long)]

    def dataTypeInfo[T: TypeInfo]: TypeInfo[DataAndWatermark[T]] = TypeInfo[DataAndWatermark[T]]
    dataTypeInfo[String]
    dataTypeInfo[(String, Int, Long)]
  }

  test("Type information equality of Either") {
    def t1 = implicitly[TypeInformation[Either[String, Int]]]
    def t2 = implicitly[TypeInformation[Either[String, Int]]]

    assertEqualDifferentReferences(t1, t2)
  }

  test("Type information equality of Option") {
    def t1 = implicitly[TypeInformation[Option[String]]]
    def t2 = implicitly[TypeInformation[Option[String]]]

    assertEqualDifferentReferences(t1, t2)
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

    assertEqualDifferentReferences(t1, t2)
  }

  test("Type information equality of non-recursive coproduct") {
    def t1 = implicitly[TypeInformation[Choice]]
    def t2 = implicitly[TypeInformation[Choice]]

    assertEqualDifferentReferences(t1, t2)
  }

  test("Type information equality of recursive coproduct") {
    def t1 = implicitly[TypeInformation[Tree[Int]]]
    def t2 = implicitly[TypeInformation[Tree[Int]]]

    assertEqualDifferentReferences(t1, t2)
  }

  test("Type information for errors or something") {
    val t1 = implicitly[TypeInformation[ValidatedNel[ErrorType, String]]]
    val t2 = implicitly[TypeInformation[ValidatedNel[ErrorType, String]]]

    assertEqualDifferentReferences(t1, t2)
  }

  def assertEqualDifferentReferences[T <: AnyRef](t1: T, t2: T): Unit = {
    assert(t1 == t2)
    assert(!(t1 eq t2))
  }
}
