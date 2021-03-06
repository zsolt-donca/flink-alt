package com.github.flinkalt.typeinfo.generic

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.serializer.{DeserializationState, SerializationState, ValueSerializer}
import com.github.flinkalt.typeinfo.{SerializerBasedTypeInfo, TypeInfo}
import shapeless.{:+:, ::, CNil, Coproduct, HList, HNil, Inl, Inr, Lazy}

trait GenTypeInfo[T] extends Serializable {
  def value: TypeInfo[T]
}

object GenTypeInfo {
  implicit def hnilTypeInfo: GenTypeInfo[HNil] = new GenTypeInfo[HNil] {
    override def value: TypeInfo[HNil] = new SerializerBasedTypeInfo[HNil] with ValueSerializer[HNil] {
      override val nestedTypeInfos: Unit = ()

      override def serializeNewValue(value: HNil, dataOutput: DataOutput, state: SerializationState): Unit = {}

      override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): HNil = HNil
    }
  }

  implicit def hlistTypeInfo[H, T <: HList](implicit headTi: Lazy[TypeInfo[H]], tailTi: GenTypeInfo[T]): GenTypeInfo[H :: T] = new GenTypeInfo[H :: T] {
    override def value: TypeInfo[H :: T] = new SerializerBasedTypeInfo[H :: T] with ValueSerializer[H :: T] {
      override lazy val nestedTypeInfos: (TypeInfo[H], TypeInfo[T]) = (headTi.value, tailTi.value)

      override def serializeNewValue(value: H :: T, dataOutput: DataOutput, state: SerializationState): Unit = {
        value match {
          case head :: tail =>
            headTi.value.serialize(head, dataOutput, state)
            tailTi.value.serializeNewValue(tail, dataOutput, state)
        }
      }

      override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): H :: T = {
        val head = headTi.value.deserialize(dataInput, state)
        val tail = tailTi.value.deserializeNewValue(dataInput, state)
        head :: tail
      }
    }
  }

  implicit def cnilTypeInfo: GenTypeInfo[CNil] = new GenTypeInfo[CNil] {
    override def value: TypeInfo[CNil] = new SerializerBasedTypeInfo[CNil] with ValueSerializer[CNil] {
      override val nestedTypeInfos: Unit = ()

      override def serializeNewValue(value: CNil, dataOutput: DataOutput, state: SerializationState): Unit = {
        sys.error("Impossible")
      }

      override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): CNil = {
        sys.error("Impossible")
      }
    }
  }

  implicit def coproductSerializer[H, T <: Coproduct](implicit headTi: Lazy[MkTypeInfo[H]], tailTi: GenTypeInfo[T]): GenTypeInfo[H :+: T] = new GenTypeInfo[H :+: T] {
    override def value: TypeInfo[H :+: T] = new SerializerBasedTypeInfo[H :+: T] with ValueSerializer[H :+: T] {
      override lazy val nestedTypeInfos: (TypeInfo[H], TypeInfo[T]) = (headTi.value.value, tailTi.value)

      override def serializeNewValue(value: H :+: T, dataOutput: DataOutput, state: SerializationState): Unit = {
        value match {
          case Inl(head) =>
            dataOutput.writeInt(state.coproductCases)
            state.coproductCases = 0
            headTi.value.value.serializeNewValue(head, dataOutput, state)
          case Inr(tail) =>
            state.coproductCases += 1
            tailTi.value.serializeNewValue(tail, dataOutput, state)
        }
      }

      override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): H :+: T = {
        if (state.withoutCoproductCases) {
          state.setCoproductCases(dataInput.readInt())
        }

        if (state.decreaseAndCheckCoproductCases) {
          Inl(headTi.value.value.deserializeNewValue(dataInput, state))
        } else {
          Inr(tailTi.value.deserializeNewValue(dataInput, state))
        }
      }
    }
  }
}
