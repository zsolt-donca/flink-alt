package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import gnu.trove.map.hash.{TIntObjectHashMap, TObjectIntHashMap}

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
    var id: Int = 0
    var isNew: Boolean = false

    var valueMap = state.objects.get(tag.runtimeClass)
    if (valueMap != null) {
      id = valueMap.get(value)
    } else {
      valueMap = new TObjectIntHashMap
      state.objects.put(tag.runtimeClass, valueMap)
    }

    if (id == 0) {
      state.id += 1
      id = state.id
      valueMap.put(value, id)
      isNew = true
    }

    dataOutput.writeInt(id)
    if (isNew) {
      serializeNewValue(value, dataOutput, state)
    }
  }

  def deserialize(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[T]): T = {
    var valueMap = state.objects.get(tag.runtimeClass)
    if (valueMap == null) {
      valueMap = new TIntObjectHashMap[Any]()
      state.objects.put(tag.runtimeClass, valueMap)
    }

    val id = dataInput.readInt()
    var value = valueMap.get(id)
    if (value == null) {
      value = deserializeNewValue(dataInput, state)
      valueMap.put(id, value)
    }


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
