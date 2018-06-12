package com.github.flinkalt.typeinfo

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.serializer._
import shapeless.{Generic, Lazy}

import scala.reflect.ClassTag

trait TypeInfo6_Generic {
  implicit def genericEncoder[T: ClassTag, R](implicit gen: Generic.Aux[T, R], genTypeInfo: Lazy[GenTypeInfo[R]]): TypeInfo[T] =
    new SerializerBasedTypeInfo[T] with RefSerializer[T] with InductiveObject {
      override val nestedTypeInfos: TypeInfo[R] = genTypeInfo.value.value

      override def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState): Unit = {
        genTypeInfo.value.value.serializeNewValue(gen.to(value), dataOutput, state)
      }

      override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): T = {
        gen.from(genTypeInfo.value.value.deserializeNewValue(dataInput, state))
      }

      override lazy val hashCode: Int = inductive[Int](0) {
        tag.hashCode() * 31 + nestedTypeInfos.hashCode()
      }

      override def equals(other: Any): Boolean = inductive[Boolean](true)(super.equals(other))
    }
}
