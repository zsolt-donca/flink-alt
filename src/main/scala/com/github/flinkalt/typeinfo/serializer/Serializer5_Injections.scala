package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

trait Serializer5_Injections extends Serializer6_Generic {
  implicit def injectSerializer[T, U](implicit inj: Injections[T, U], ser: Serializer[U]): Serializer[T] = new RefSerializer[T] {
    override def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState): Unit = {
      ser.serialize(inj(value), dataOutput, state)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): T = {
      val u = ser.deserialize(dataInput, state)
      inj.invert(u)
    }
  }
}
