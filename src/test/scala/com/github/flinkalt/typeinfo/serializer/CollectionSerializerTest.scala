package com.github.flinkalt.typeinfo.serializer

import org.scalatest.PropSpec

import scala.collection.immutable.ListMap

class CollectionSerializerTest extends PropSpec with RefSerializerHelper {
  property("Lists of ints are serialized") {
    forAllRoundTrip[List[Int]]()
  }

  property("Vectors of ints are serialized") {
    forAllRoundTrip[Vector[Int]]()
  }

  property("Sets of strings are serialized") {
    forAllRoundTrip[Vector[String]]()
  }

  property("Maps of strings are serialized") {
    forAllRoundTrip[Map[String, String]]()
  }

  property("List Maps of strings are serialized") {
    forAll { value: ListMap[String, String] =>
      val that = roundTrip(value)

      assert(value.toVector == that.toVector)
    }
  }

  // TODO make sure to properly check arrays for equality
  //  property("Arrays of bytes are serialized") {
  //    forAllRoundTrip[Array[Byte]]()
  //  }

  // TODO write instances for additional arrays
  //  property("Arrays of longs are serialized") {
  //    forAllRoundTrip[Array[Long]]()
  //  }

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

}
