package com.github.flinkalt.typeinfo.serializer

import cats.data.{NonEmptyList, Validated}
import com.github.flinkalt.typeinfo.TypeInfo
import org.scalatest.PropSpec

class ReferenceSerializerTest extends PropSpec with RefSerializerHelper {

  property("Empty product") {
    case class EmptyCaseClass()
    forAllRoundTrip[EmptyCaseClass]()
  }

  property("Product with Int") {
    case class CaseClass(int: Int)
    forAllRoundTrip[CaseClass]()
  }

  property("Product with String") {
    case class CaseClass(s: String)
    forAllRoundTrip[CaseClass]()
  }

  property("Product with Int and String") {
    case class CaseClass(i: Int, s: String)
    forAllRoundTrip[CaseClass]()
  }

  property("Product with two Ints and a String") {
    case class CaseClass(i1: Int, i2: Int, s: String)
    forAllRoundTrip[CaseClass]()
  }

  property("Product with another product") {
    case class AnotherCaseClass(i: Int, s: String)
    case class CaseClass(i1: Int, c: AnotherCaseClass)
    forAllRoundTrip[CaseClass]()
  }

  property("Coproduct with one case") {
    sealed trait SealedTrait
    case class CaseOne(int: Int) extends SealedTrait

    forAllRoundTrip[SealedTrait]()
  }

  property("Coproduct with two cases") {
    sealed trait SealedTrait
    case class CaseOne(int: Int) extends SealedTrait
    case class CaseTwo(s: String, i: Int) extends SealedTrait

    forAllRoundTrip[SealedTrait]()
  }

  property("Coproduct with two three") {
    sealed trait SealedTrait
    case class CaseOne(int: Int) extends SealedTrait
    case class CaseTwo(s: String, i: Int) extends SealedTrait
    case class CaseThree(s: String, i: Int, l: Long, u: Unit) extends SealedTrait

    forAllRoundTrip[SealedTrait]()
  }

  property("Coproduct with another Coproduct") {
    sealed trait SealedTrait
    case class CaseOne(int: Int) extends SealedTrait
    case class CaseTwo(s: String, c: AnotherSealedTrait) extends SealedTrait

    sealed trait AnotherSealedTrait
    case class CaseThree(i: Int) extends AnotherSealedTrait
    case object CaseFour extends AnotherSealedTrait

    forAllRoundTrip[SealedTrait]()
  }

  property("Equal strings should be same when deserialized") {
    forAllRoundTripWithPair[String]()
  }

  property("Equal products should be same when deserialized") {
    case class CaseClass(i: Int)

    forAllRoundTripWithPair[CaseClass]()
  }

  property("Equal coproducts should be same when deserialized") {
    sealed trait SealedTrait
    case class CaseOne(int: Int) extends SealedTrait

    forAllRoundTripWithPair[SealedTrait]()
  }

  property("Serialize a recursive coproduct") {
    sealed trait Tree[A]
    case class Node[A](left: Tree[A], right: Tree[A]) extends Tree[A]
    case class Leaf[A](value: A) extends Tree[A]

    forAllRoundTrip[Tree[Int]]()
  }

  property("Serialize a recursive product") {
    case class Tree[+E](value: E, children: List[Tree[E]])

    forAllRoundTrip[Tree[Int]]()
  }

  property("Serialize an either") {
    assert(TypeInfo[Either[String, Either[Int, Long]]] == TypeInfo.eitherTypeInfo[String, Either[Int, Long]])

    forAllRoundTrip[Either[String, Either[Int, Long]]]()
  }

  property("Serialize a validated") {
    assert(TypeInfo[Validated[NonEmptyList[String], Long]] == TypeInfo.validatedTypeInfo[NonEmptyList[String], Long])

    forAllRoundTrip[Validated[NonEmptyList[String], Long]]()
  }
}
