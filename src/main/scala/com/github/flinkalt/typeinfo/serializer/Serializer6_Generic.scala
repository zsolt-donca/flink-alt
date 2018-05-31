package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import shapeless._

trait Serializer6_Generic {

  def hnilSerializer: Serializer[HNil] = new ValueSerializer[HNil] {
    override def serializeNewValue(value: HNil, dataOutput: DataOutput, state: SerializationState): Unit = {}

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): HNil = HNil
  }

  def hlistSerializer[H, T <: HList](headSer: => Serializer[H], tailSer: => Serializer[T]): Serializer[H :: T] = new ValueSerializer[H :: T] {
    override def serializeNewValue(value: H :: T, dataOutput: DataOutput, state: SerializationState): Unit = {
      value match {
        case head :: tail =>
          headSer.serialize(head, dataOutput, state)
          tailSer.serializeNewValue(tail, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): H :: T = {
      val head = headSer.deserialize(dataInput, state)
      val tail = tailSer.deserializeNewValue(dataInput, state)
      head :: tail
    }
  }


  def cnilSerializer: Serializer[CNil] = new ValueSerializer[CNil] {
    override def serializeNewValue(value: CNil, dataOutput: DataOutput, state: SerializationState): Unit = {
      sys.error("Impossible")
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): CNil = {
      sys.error("Impossible")
    }
  }

  def coproductSerializer[H, T <: Coproduct](headSer: => Serializer[H], tailSer: => Serializer[T]): Serializer[H :+: T] = new ValueSerializer[H :+: T] {
    override def serializeNewValue(value: H :+: T, dataOutput: DataOutput, state: SerializationState): Unit = {
      value match {
        case Inl(head) =>
          dataOutput.writeInt(state.readeAndResetCoproductCases)
          headSer.serializeNewValue(head, dataOutput, state)
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
        Inl(headSer.deserializeNewValue(dataInput, state))
      } else {
        Inr(tailSer.deserializeNewValue(dataInput, state))
      }
    }
  }


  def genericSerializer[A, R](gen: Generic.Aux[A, R], ser: Serializer[R]): Serializer[A] = new RefSerializer[A] {
    override def serializeNewValue(value: A, dataOutput: DataOutput, state: SerializationState): Unit = {
      val r = gen.to(value)
      ser.serializeNewValue(r, dataOutput, state)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): A = {
      val r = ser.deserializeNewValue(dataInput, state)
      gen.from(r)
    }
  }
}
