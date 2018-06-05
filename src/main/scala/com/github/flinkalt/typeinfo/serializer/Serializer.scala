package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import scala.reflect.ClassTag

trait Serializer[T] extends Serializable {
  self =>
  def serialize(value: T, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[T]): Unit

  def deserialize(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[T]): T

  def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[T]): Unit

  def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[T]): T

  def imap[U](to: T => U)(from: U => T)(implicit tt: ClassTag[T], ut: ClassTag[U]): Serializer[U] = new Serializer[U] {
    override def serialize(value: U, dataOutput: DataOutput, state: SerializationState)(implicit bt: ClassTag[U]): Unit = {
      self.serialize(from(value), dataOutput, state)
    }

    override def deserialize(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[U]): U = {
      to(self.deserialize(dataInput, state))
    }

    override def serializeNewValue(value: U, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[U]): Unit = {
      self.serializeNewValue(from(value), dataOutput, state)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[U]): U = {
      to(self.deserializeNewValue(dataInput, state))
    }
  }
}

object Serializer extends Serializer1_Primitives {
  type RefId = Int
}

trait RefSerializer[T] extends Serializer[T] {
  def serialize(value: T, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[T]): Unit = {
    val maybeId = state.get(value)
    val id = maybeId.getOrElse(state.put(value))
    dataOutput.writeInt(id)

    if (maybeId.isEmpty) {
      serializeNewValue(value, dataOutput, state)
    }
  }

  def deserialize(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[T]): T = {
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
    override def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[T]): Unit = write(dataOutput, value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[T]): T = read(dataInput)
  }
}


trait ValueSerializer[T] extends Serializer[T] {
  def serialize(value: T, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[T]): Unit = serializeNewValue(value, dataOutput, state)

  def deserialize(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[T]): T = deserializeNewValue(dataInput, state)
}

object ValueSerializer {
  def apply[T](write: (DataOutput, T) => Unit, read: DataInput => T): ValueSerializer[T] = new ValueSerializer[T] {
    override def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[T]): Unit = write(dataOutput, value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[T]): T = read(dataInput)
  }
}
