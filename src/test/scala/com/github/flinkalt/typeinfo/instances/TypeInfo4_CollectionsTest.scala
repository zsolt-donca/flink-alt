package com.github.flinkalt.typeinfo.instances

import com.github.flinkalt.typeinfo.auto._
import org.scalatest.PropSpec

import scala.collection.immutable.ListMap

class TypeInfo4_CollectionsTest extends PropSpec with RefSerializerHelper {
  property("Lists of ints are serialized") {
    forAllRoundTrip[List[Int]]()
  }

  property("Vectors of ints are serialized") {
    forAllRoundTrip[Vector[Int]]()
  }

  property("Vectors of strings are serialized") {
    forAllRoundTrip[Vector[String]]()
  }

  property("Seqs of strings are serialized") {
    forAllRoundTrip[Seq[String]]()
  }

  property("IndexedSeqs of strings are serialized") {
    forAllRoundTrip[IndexedSeq[String]]()
  }

  property("Sets of strings are serialized") {
    forAllRoundTrip[Set[String]]()
  }

  property("Maps of strings are serialized") {
    forAllRoundTrip[Map[String, String]]()
  }

  property("ListMaps of strings are serialized") {
    forAllRoundTrip[ListMap[String, String]]()
  }

  property("Vectors of products are serialized") {
    case class Test(i: Int, c: Char)
    forAllRoundTrip[Vector[Test]]()
  }

  property("Vectors of coproducts are serialized") {
    sealed trait Coproduct
    case class CaseOne(i: Int) extends Coproduct
    case class CaseTwo() extends Coproduct
    case object CaseThree extends Coproduct
    forAllRoundTrip[Vector[Coproduct]]()
  }

  case class Test(list: List[Int], vector: Vector[Int])

  property("Collections of different types are serialized correctly") {
    forAllRoundTrip[Test]()
  }

  property("Empty equal collections of different types are serialized correctly") {
    roundTrip(Test(List.empty, Vector.empty))
  }

  property("Non-empty equal collections of different types are serialized correctly") {
    roundTrip(Test(List(42), Vector(42)))
  }
}
