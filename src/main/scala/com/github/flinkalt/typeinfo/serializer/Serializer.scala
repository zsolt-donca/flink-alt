package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import cats.Invariant

trait Serializer[T] extends Serializable {
  def serialize(value: T, dataOutput: DataOutput, state: SerializationState): Unit

  def deserialize(dataInput: DataInput, state: DeserializationState): T

  def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState): Unit

  def deserializeNewValue(dataInput: DataInput, state: DeserializationState): T
}

object Serializer extends Serializer1_Primitives {
  type RefId = Int

  implicit def invariantSerializer[T]: Invariant[Serializer] = new Invariant[Serializer] {
    override def imap[A, B](fa: Serializer[A])(to: A => B)(from: B => A): Serializer[B] = new Serializer[B] {
      override def serialize(value: B, dataOutput: DataOutput, state: SerializationState): Unit = {
        fa.serialize(from(value), dataOutput, state)
      }

      override def deserialize(dataInput: DataInput, state: DeserializationState): B = {
        to(fa.deserialize(dataInput, state))
      }

      override def serializeNewValue(value: B, dataOutput: DataOutput, state: SerializationState): Unit = {
        fa.serializeNewValue(from(value), dataOutput, state)
      }

      override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): B = {
        to(fa.deserializeNewValue(dataInput, state))
      }
    }
  }
}

trait RefSerializer[T] extends Serializer[T] {
  def serialize(value: T, dataOutput: DataOutput, state: SerializationState): Unit = {
    val maybeId = state.get(value)
    val id = maybeId.getOrElse(state.put(value))
    dataOutput.writeInt(id)

    if (maybeId.isEmpty) {
      serializeNewValue(value, dataOutput, state)
    }
  }

  def deserialize(dataInput: DataInput, state: DeserializationState): T = {
    val id = dataInput.readInt()
    val maybeValue = state.get(id)
    val value = maybeValue.getOrElse({
      val value = deserializeNewValue(dataInput, state)
      state.put(id, value)
      value
    })

    value.asInstanceOf[T]
  }
}

object RefSerializer {
  def apply[T <: AnyRef](write: (DataOutput, T) => Unit, read: DataInput => T): RefSerializer[T] = new RefSerializer[T] {
    override def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState): Unit = write(dataOutput, value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): T = read(dataInput)
  }
}


trait ValueSerializer[T] extends Serializer[T] {
  def serialize(value: T, dataOutput: DataOutput, state: SerializationState): Unit = serializeNewValue(value, dataOutput, state)

  def deserialize(dataInput: DataInput, state: DeserializationState): T = deserializeNewValue(dataInput, state)
}

object ValueSerializer {
  def apply[T](write: (DataOutput, T) => Unit, read: DataInput => T): ValueSerializer[T] = new ValueSerializer[T] {
    override def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState): Unit = write(dataOutput, value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): T = read(dataInput)
  }
}
