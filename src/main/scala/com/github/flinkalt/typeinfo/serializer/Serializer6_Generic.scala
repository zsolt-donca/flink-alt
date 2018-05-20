package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import shapeless._

trait Serializer6_Generic {

  implicit def hnilSerializer: Serializer[HNil] = new ValueSerializer[HNil] {
    override def serializeNewValue(value: HNil, dataOutput: DataOutput, state: SerializationState): Unit = {}

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): HNil = HNil
  }

  implicit def hlistSerializer[H, T <: HList](implicit headSer: Lazy[Serializer[H]], tailSer: Serializer[T]): Serializer[H :: T] = new ValueSerializer[H :: T] {
    override def serializeNewValue(value: H :: T, dataOutput: DataOutput, state: SerializationState): Unit = {
      value match {
        case head :: tail =>
          headSer.value.serialize(head, dataOutput, state)
          tailSer.serializeNewValue(tail, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): H :: T = {
      val head = headSer.value.deserialize(dataInput, state)
      val tail = tailSer.deserializeNewValue(dataInput, state)
      head :: tail
    }
  }


  implicit def cnilSerializer: Serializer[CNil] = new ValueSerializer[CNil] {
    override def serializeNewValue(value: CNil, dataOutput: DataOutput, state: SerializationState): Unit = {
      sys.error("Impossible")
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): CNil = {
      sys.error("Impossible")
    }
  }

  implicit def coproductSerializer[H, T <: Coproduct](implicit headSer: Lazy[Serializer[H]], tailSer: Serializer[T]): Serializer[H :+: T] = new ValueSerializer[H :+: T] {
    override def serializeNewValue(value: H :+: T, dataOutput: DataOutput, state: SerializationState): Unit = {
      value match {
        case Inl(head) =>
          dataOutput.writeInt(state.readeAndResetCoproductCases)
          headSer.value.serializeNewValue(head, dataOutput, state)
        case Inr(tail) =>
          state.increaseCoproductCases()
          tailSer.serializeNewValue(tail, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): H :+: T = {
      if (state.withoutCoproductCases) {
        state.setCoproductCases(dataInput.readInt())
      }

      if (state.decreaseAndCheckCoproductCases) {
        Inl(headSer.value.deserializeNewValue(dataInput, state))
      } else {
        Inr(tailSer.deserializeNewValue(dataInput, state))
      }
    }
  }


  implicit def genericEncoder[A, R](implicit gen: Generic.Aux[A, R], ser: Lazy[Serializer[R]]): Serializer[A] = new RefSerializer[A] {
    override def serializeNewValue(value: A, dataOutput: DataOutput, state: SerializationState): Unit = {
      val r = gen.to(value)
      ser.value.serializeNewValue(r, dataOutput, state)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): A = {
      val r = ser.value.deserializeNewValue(dataInput, state)
      gen.from(r)
    }
  }
}
