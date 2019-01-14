package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import gnu.trove.map.hash.TObjectIntHashMap

trait Serializer[T] extends Serializable {
  self =>

  def serialize(value: T, dataOutput: DataOutput, state: SerializationState): Unit

  def deserialize(dataInput: DataInput, state: DeserializationState): T

  def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState): Unit

  def deserializeNewValue(dataInput: DataInput, state: DeserializationState): T
}

trait RefSerializer[T] extends Serializer[T] {
  def serialize(value: T, dataOutput: DataOutput, state: SerializationState): Unit = {
    var id: Int = 0
    var isNew: Boolean = false

    if (value == null) {
      id = -1
    } else {
      var valueMap = state.objects.get(this)
      if (valueMap != null) {
        id = valueMap.get(value)
      } else {
        valueMap = new TObjectIntHashMap
        state.objects.put(this, valueMap)
      }
      if (id == 0) {
        state.lastId += 1
        id = state.lastId
        valueMap.put(value, id)
        isNew = true
      }
    }

    dataOutput.writeInt(id)
    if (isNew) {
      serializeNewValue(value, dataOutput, state)
    }
  }

  def deserialize(dataInput: DataInput, state: DeserializationState): T = {
    val id = dataInput.readInt()

    if (id == -1) {
      null.asInstanceOf[T]
    } else {
      var value = state.objects.get(id)
      if (value == null) {
        value = deserializeNewValue(dataInput, state)
        state.objects.put(id, value)
      }

      value.asInstanceOf[T]
    }
  }
}

trait LeafRefSerializer[T <: AnyRef] extends RefSerializer[T] {
  def write(dataOutput: DataOutput, value: T): Unit

  def read(dataInput: DataInput): T

  final override def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState): Unit = write(dataOutput, value)

  final override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): T = read(dataInput)
}

trait ValueSerializer[T] extends Serializer[T] {
  final def serialize(value: T, dataOutput: DataOutput, state: SerializationState): Unit = serializeNewValue(value, dataOutput, state)

  final def deserialize(dataInput: DataInput, state: DeserializationState): T = deserializeNewValue(dataInput, state)
}
