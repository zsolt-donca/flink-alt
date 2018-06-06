package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.auto._
import org.scalatest.FunSuite

case class Test(list: List[Int], vector: Vector[Int])

class SerializerTest extends FunSuite with RefSerializerHelper {

  case class Tree[+E](value: E, children: List[Tree[E]])
  
  test("Empty equal collections of different types are serialized correctly") {
    roundTripWithSerializer(Test(List.empty, Vector.empty))
  }

  test("Non-empty equal collections of different types are serialized correctly") {
    roundTripWithSerializer(Test(List(42), Vector(42)))
  }

  test("Recursive coproduct") {
    val value = Tree("foo", List(Tree("bar", List())))
    roundTripWithSerializer(value)
  }
}
