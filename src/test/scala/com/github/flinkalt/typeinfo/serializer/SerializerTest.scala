package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.auto._
import org.scalatest.FunSuite

case class Test(list: List[Int], vector: Vector[Int])

class SerializerTest extends FunSuite with RefSerializerHelper {
  test("Empty equal collections of different types are serialized correctly") {
    roundTripWithSerializer(Test(List.empty, Vector.empty))
  }

  test("Non-empty equal collections of different types are serialized correctly") {
    roundTripWithSerializer(Test(List(42), Vector(42)))
  }
}
