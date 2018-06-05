package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.Injection

import scala.reflect.ClassTag

trait Serializer5_Injections extends Serializer6_Generic {
  def injectSerializer[T, U: ClassTag](inj: Injection[T, U], ser: Serializer[U]): Serializer[T] = new RefSerializer[T] {
    override def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[T]): Unit = {
      ser.serialize(inj(value), dataOutput, state)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[T]): T = {
      val u = ser.deserialize(dataInput, state)
      inj.invert(u)
    }
  }
}
