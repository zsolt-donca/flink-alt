package com.github.flinkalt.typeinfo.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.scalacheck.{Arbitrary, ScalacheckShapeless}
import org.scalatest.Assertions
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import shapeless.{Coproduct, HList}

trait RefSerializerHelper extends GeneratorDrivenPropertyChecks with Assertions with ScalacheckShapeless {

  def forAllRoundTrip[T <: AnyRef : Serializer : Arbitrary](): Unit = {
    forAll { value: T =>
      roundTrip(value)
    }
  }

  case class Pair[T](p1: T, p2: T)

  def forAllRoundTripWithPair[T <: AnyRef : Serializer : Arbitrary](): Unit = {
    forAll { value: T =>
      val copy = roundTrip(Pair(value, value))
      assert(copy.p1 eq copy.p2)
    }
  }

  def roundTrip[T <: AnyRef : Serializer : Arbitrary](value: T): T = {
    val ser = implicitly[Serializer[T]]
    val bos = new ByteArrayOutputStream()
    val dataOutput = new DataOutputStream(bos)
    val state = new SerializationState
    ser.serialize(value, dataOutput, state)

    val bytes = bos.toByteArray
    val bis = new ByteArrayInputStream(bytes)
    val dataInput = new DataInputStream(bis)
    val copy = ser.deserialize(dataInput, new DeserializationState)

    //    assert(value ne copy, "The deserialized object is somehow the same instance as the original.")
    assert(value == copy, "The deserialized object is not equal to the original.")
    assert(bis.available == 0, "The deserialization did not consume all data.")
    assert(!state.values.exists(e => e.isInstanceOf[HList] || e.isInstanceOf[Coproduct]), "The generic representation was somehow serialized.")

    copy
  }

}
