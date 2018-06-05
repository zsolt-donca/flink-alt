package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.TypeInfo
import shapeless._

import scala.reflect.ClassTag

trait Serializer6_Generic {

  def hnilSerializer: Serializer[HNil] = new ValueSerializer[HNil] {
    override def serializeNewValue(value: HNil, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[HNil]): Unit = {}

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[HNil]): HNil = HNil
  }

  def hlistSerializer[H, T <: HList](headTi: => TypeInfo[H], tailTi: => TypeInfo[T]): Serializer[H :: T] = new ValueSerializer[H :: T] {
    override def serializeNewValue(value: H :: T, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[H :: T]): Unit = {
      value match {
        case head :: tail =>
          headTi.serializer.serialize(head, dataOutput, state)(headTi.tag)
          tailTi.serializer.serializeNewValue(tail, dataOutput, state)(tailTi.tag)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[H :: T]): H :: T = {
      val head = headTi.serializer.deserialize(dataInput, state)(headTi.tag)
      val tail = tailTi.serializer.deserializeNewValue(dataInput, state)(tailTi.tag)
      head :: tail
    }
  }


  def cnilSerializer: Serializer[CNil] = new ValueSerializer[CNil] {
    override def serializeNewValue(value: CNil, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[CNil]): Unit = {
      sys.error("Impossible")
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[CNil]): CNil = {
      sys.error("Impossible")
    }
  }

  def coproductSerializer[H, T <: Coproduct](headTi: => TypeInfo[H], tailTi: => TypeInfo[T]): Serializer[H :+: T] = new ValueSerializer[H :+: T] {
    override def serializeNewValue(value: H :+: T, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[H :+: T]): Unit = {
      value match {
        case Inl(head) =>
          dataOutput.writeInt(state.readeAndResetCoproductCases)
          headTi.serializer.serializeNewValue(head, dataOutput, state)(headTi.tag)
        case Inr(tail) =>
          state.increaseCoproductCases()
          tailTi.serializer.serializeNewValue(tail, dataOutput, state)(tailTi.tag)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[H :+: T]): H :+: T = {
      if (state.withoutCoproductCases) {
        state.setCoproductCases(dataInput.readInt())
      }

      if (state.decreaseAndCheckCoproductCases) {
        Inl(headTi.serializer.deserializeNewValue(dataInput, state)(headTi.tag))
      } else {
        Inr(tailTi.serializer.deserializeNewValue(dataInput, state)(tailTi.tag))
      }
    }
  }


  def genericSerializer[A, R](gen: Generic.Aux[A, R], typeInfo: TypeInfo[R]): Serializer[A] = new RefSerializer[A] {
    override def serializeNewValue(value: A, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[A]): Unit = {
      val r = gen.to(value)
      typeInfo.serializer.serializeNewValue(r, dataOutput, state)(typeInfo.tag)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[A]): A = {
      val r = typeInfo.serializer.deserializeNewValue(dataInput, state)(typeInfo.tag)
      gen.from(r)
    }
  }
}
