package com.github.flinkalt.typeinfo.instances

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.auto._
import com.github.flinkalt.typeinfo.serializer.{DeserializationState, SerializationState}
import org.scalacheck.{Arbitrary, ScalacheckShapeless}
import org.scalatest.Assertions
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import shapeless.{Coproduct, HList}

import scala.collection.JavaConverters._

trait RefSerializerHelper extends GeneratorDrivenPropertyChecks with Assertions with ScalacheckShapeless {

  def forAllRoundTrip[T <: AnyRef : TypeInfo : Arbitrary](): Unit = {
    forAll { value: T =>
      roundTrip(value)
    }

    val t: T = null.asInstanceOf[T]
    roundTrip[T](t)
  }

  case class Pair[T](p1: T, p2: T)

  def forAllRoundTripWithPair[T <: AnyRef : TypeInfo : Arbitrary](): Unit = {
    forAll { value: T =>
      val copy = roundTrip(Pair(value, value))
      assert(copy.p1 eq copy.p2)
    }
  }

  def roundTrip[T <: AnyRef : TypeInfo](value: T): T = {
    val typeInfo = TypeInfo[T]
    val ser = typeInfo
    val bos = new ByteArrayOutputStream()
    val dataOutput = new DataOutputStream(bos)
    val state = new SerializationState
    ser.serialize(value, dataOutput, state)

    val bytes = bos.toByteArray
    val bis = new ByteArrayInputStream(bytes)
    val dataInput = new DataInputStream(bis)
    val copy = ser.deserialize(dataInput, new DeserializationState)

    (value, copy) match {
      case (valueArray: Array[_], copyArray: Array[_]) =>
        assert(valueArray.toVector == copyArray.toVector, "The deserialized object is not equal to the original.")

      case _ =>
        assert(value == copy, "The deserialized object is not equal to the original.")
    }

    assert(bis.available == 0, "The deserialization did not consume all data.")

    val values = state.objects.asScala.values.flatMap(_.keys()).toVector
    assert(!values.exists(e => e.isInstanceOf[HList] || e.isInstanceOf[Coproduct]), "The generic representation was somehow serialized.")

    copy
  }

}
