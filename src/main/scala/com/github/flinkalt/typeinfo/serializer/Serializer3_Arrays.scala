package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import scala.reflect.ClassTag

trait Serializer3_Arrays extends Serializer4_Collections {

  def byteArraySerializer: Serializer[Array[Byte]] = new ValueSerializer[Array[Byte]] {
    override def serializeNewValue(value: Array[Byte], dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[Array[Byte]]): Unit = {
      dataOutput.writeInt(value.length)
      dataOutput.write(value)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[Array[Byte]]): Array[Byte] = {
      val length = dataInput.readInt()
      val array = new Array[Byte](length)
      dataInput.readFully(array)
      array
    }
  }

  // TODO add further array types, such as Long
}
