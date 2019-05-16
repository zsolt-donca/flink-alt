package com.github.flinkalt.typeinfo.instances

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.serializer.{DeserializationState, RefSerializer, SerializationState}
import com.github.flinkalt.typeinfo.{SerializerBasedTypeInfo, TypeInfo}

import scala.reflect.ClassTag

trait TypeInfo6_Tuples {
  implicit def tuple2TypeInfo[A: TypeInfo : ClassTag, B: TypeInfo : ClassTag]: TypeInfo[(A, B)] = new SerializerBasedTypeInfo[(A, B)] with RefSerializer[(A, B)] {
    override def nestedTypeInfos: (TypeInfo[A], TypeInfo[B]) = (TypeInfo[A], TypeInfo[B])

    override def serializeNewValue(value: (A, B), dataOutput: DataOutput, state: SerializationState): Unit = {
      TypeInfo[A].serialize(value._1, dataOutput, state)
      TypeInfo[B].serialize(value._2, dataOutput, state)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): (A, B) = {
      (
        TypeInfo[A].deserialize(dataInput, state),
        TypeInfo[B].deserialize(dataInput, state),
      )
    }
  }

  implicit def tuple3TypeInfo[A: TypeInfo : ClassTag, B: TypeInfo : ClassTag, C: TypeInfo : ClassTag]: TypeInfo[(A, B, C)] = new SerializerBasedTypeInfo[(A, B, C)] with RefSerializer[(A, B, C)] {
    override def nestedTypeInfos: (TypeInfo[A], TypeInfo[B], TypeInfo[C]) = (TypeInfo[A], TypeInfo[B], TypeInfo[C])

    override def serializeNewValue(value: (A, B, C), dataOutput: DataOutput, state: SerializationState): Unit = {
      TypeInfo[A].serialize(value._1, dataOutput, state)
      TypeInfo[B].serialize(value._2, dataOutput, state)
      TypeInfo[C].serialize(value._3, dataOutput, state)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): (A, B, C) = {
      (
        TypeInfo[A].deserialize(dataInput, state),
        TypeInfo[B].deserialize(dataInput, state),
        TypeInfo[C].deserialize(dataInput, state),
      )
    }
  }
}
