package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.auto._
import org.scalatest.PropSpec

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

  property("Arrays of bytes are serialized") {
    forAllRoundTrip[Array[Byte]]()
  }

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
