package com.github.flinkalt.typeinfo.desc

import com.github.flinkalt.typeinfo.Injection
import org.scalatest.{FunSuite, Inside, Matchers}

class TypeDescTest extends FunSuite with Matchers with Inside {
  test("Primitive type descriptors") {
    assert(TypeDesc[Boolean] == BooleanTypeDesc)
    assert(TypeDesc[Byte] == ByteTypeDesc)
    assert(TypeDesc[Short] == ShortTypeDesc)
    assert(TypeDesc[Char] == CharTypeDesc)
    assert(TypeDesc[Int] == IntTypeDesc)
    assert(TypeDesc[Long] == LongTypeDesc)
    assert(TypeDesc[Float] == FloatTypeDesc)
    assert(TypeDesc[Double] == DoubleTypeDesc)
    assert(TypeDesc[Unit] == UnitTypeDesc)
    assert(TypeDesc[BigInt] == BigIntTypeDesc)
  }

  test("Common type descriptors") {
    assert(TypeDesc[String] == StringTypeDesc)
  }

  test("Array type descriptors") {
    assert(TypeDesc[Array[Byte]] == ByteArrayTypeDesc)
  }

  test("Traversable type descriptors") {
    assert(TypeDesc[List[String]] == TraversableType[List, String](StringTypeDesc))
    assert(TypeDesc[Set[List[Long]]] == TraversableType[Set, List[Long]](TraversableType[List, Long](LongTypeDesc)))
  }

  test("Injection type descriptors") {

    class Test1(val str: String)
    implicit def injectTest1ToString: Injection[Test1, String] = Injection[Test1, String](_.str, s => new Test1(s))

    val actualTypeDesc = TypeDesc[Test1]

    actualTypeDesc should matchPattern { case InjectionTypeDesc(_, StringTypeDesc) => }
  }

  test("Generic type descriptor") {
    case class P1(p: String, i: Int)
    case class P2(l: Long, p1: P1)

    TypeDesc[P1] should matchPattern { case GenericTypeDesc() => }
    TypeDesc[P2] should matchPattern { case GenericTypeDesc() => }

    sealed trait C1
    case object Case1 extends C1
    case object Case2 extends C1
    case class Case3(i: Int, l: Long) extends C1

    TypeDesc[C1] should matchPattern { case GenericTypeDesc() => }

    // to avoid errors that these classes are unused
    assert(P1.getClass != null)
    assert(P2.getClass != null)
    assert(Case1.getClass != null)
    assert(Case2.getClass != null)
    assert(Case3.getClass != null)
  }
}
