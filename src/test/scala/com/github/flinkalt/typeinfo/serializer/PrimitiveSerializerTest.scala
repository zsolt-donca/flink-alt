package com.github.flinkalt.typeinfo.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.scalacheck.Arbitrary
import org.scalacheck.ScalacheckShapeless._
import org.scalatest.PropSpec
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import shapeless.{Coproduct, HList, the}

import scala.reflect.ClassTag

class PrimitiveSerializerTest extends PropSpec with GeneratorDrivenPropertyChecks with Serializer1_Primitives {

  property("Booleans are serialized plainly") {
    forAllRoundTrip[Boolean]()
  }

  property("Bytes are serialized plainly") {
    forAllRoundTrip[Byte]()
  }

  property("Short are serialized plainly") {
    forAllRoundTrip[Short]()
  }

  property("Chars are serialized plainly") {
    forAllRoundTrip[Char]()
  }

  property("Ints are serialized plainly") {
    forAllRoundTrip[Int]()
  }

  property("Longs are serialized plainly") {
    forAllRoundTrip[Long]()
  }

  property("Float are serialized plainly") {
    forAllRoundTrip[Float]()
  }

  property("Double are serialized plainly") {
    forAllRoundTrip[Double]()
  }

  property("Unit are serialized plainly") {
    forAllRoundTrip[Unit]()
  }

  def forAllRoundTrip[T <: AnyVal : ClassTag : Serializer : Arbitrary](): Unit = {
    forAll { value: T =>
      roundTrip(value)
    }
  }

  private def roundTrip[T <: AnyVal : ClassTag : Serializer : Arbitrary](value: T): Unit = {
    val ser = implicitly[Serializer[T]]
    val bos = new ByteArrayOutputStream()
    val dataOutput = new DataOutputStream(bos)
    val state = new SerializationState
    ser.serialize(value, dataOutput, state)

    val bytes = bos.toByteArray
    val bis = new ByteArrayInputStream(bytes)
    val dataInput = new DataInputStream(bis)
    val copy = ser.deserialize(dataInput, new DeserializationState)

    assert(bytes.length == expectedSizeOf[T])
    assert(value == copy, "The deserialized object is not equal to the original.")
    assert(bis.available == 0, "The deserialization did not consume all data.")
    assert(!state.values.exists(e => e.isInstanceOf[HList] || e.isInstanceOf[Coproduct]), "The generic representation was somehow serialized.")
  }

  private def expectedSizeOf[T <: AnyVal : ClassTag]: Int = {
    val clazz = the[ClassTag[T]].runtimeClass
    assert(clazz.isPrimitive)

    val sizes = Map[Class[_], Int](
      classOf[Unit] -> 0,
      classOf[Boolean] -> 1,
      classOf[Byte] -> 1,
      classOf[Short] -> 2,
      classOf[Char] -> 2,
      classOf[Int] -> 4,
      classOf[Long] -> 8,
      classOf[Float] -> 4,
      classOf[Double] -> 8
    )

    sizes.getOrElse(clazz, sys.error(s"Not a primitive: $clazz"))
  }

}
