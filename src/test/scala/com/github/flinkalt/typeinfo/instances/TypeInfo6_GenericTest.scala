package com.github.flinkalt.typeinfo.instances

import com.github.flinkalt.Data
import com.github.flinkalt.typeinfo.auto._
import org.scalatest.PropSpec

class TypeInfo6_GenericTest extends PropSpec with RefSerializerHelper {

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

  property("Tuple with multiple components") {
    forAllRoundTrip[(String, Long, Int)]()
  }

  property("Envelope with stuff in it") {
    case class Envelope[T](s: String, i: Int, contents: T)
    forAllRoundTrip[Envelope[Data[(String, Int)]]]()
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

  property("Serialize a case class with options of different types") {
    case class Foo(a: Option[Int], b: Option[Double])

    val copy = roundTripWithSerializer(Foo(Some(0), Some(0.0)))
    val i: Int = copy.a.get
    val d: Double = copy.b.get

    assert(i == 0)
    assert(d == 0.0)
  }

  property("Serialize a collection with different but equal contents") {
    case class Foo(a: List[Vector[Int]], b: Vector[List[Int]])

    val value = Foo(List(Vector(1)), Vector(List(1)))
    val copy = roundTripWithSerializer(value)

    val a: List[Vector[Int]] = copy.a
    val ah: Vector[Int] = a.head
    val ahh: Int = ah.head
    assert(ahh == 1)

    val b = copy.b
    val bh: List[Int] = b.head
    val bhh = bh.head
    assert(bhh == 1)
  }

  property("Serialize some generic type with different but equal arguments") {
    case class Foo[T](value: T)
    case class Bar(i: Foo[Int], d: Foo[Double])

    val value = Bar(Foo(0), Foo(0.0))
    val copy = roundTripWithSerializer(value)

    assert(copy.i.value == 0)
    assert(copy.d.value == 0.0)
  }

  property("Serialize some non-generic type with different but equal arguments") {
    case class Bar(i: Int, d: Double)

    val value = Bar(0, 0.0)
    val copy = roundTripWithSerializer(value)

    assert(copy.i == 0)
    assert(copy.d == 0.0)
  }

  case class Tree[+E](value: E, children: List[Tree[E]])

  property("Serialize recursive coproducts") {
    forAllRoundTrip[Tree[String]]()
  }

  property("Serialize a particular recursive coproduct") {
    val value = Tree("foo", List(Tree("bar", List())))
    roundTripWithSerializer(value)
  }
}
