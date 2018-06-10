package com.github.flinkalt.typeinfo

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.serializer.{DeserializationState, SerializationState, ValueSerializer}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo

trait TypeInfo3_Arrays extends TypeInfo4_Collections {
  implicit def byteArrayTypeInfo: TypeInfo[Array[Byte]] = new DirectTypeInfo(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO) with ValueSerializer[Array[Byte]] {
    override def serializeNewValue(value: Array[Byte], dataOutput: DataOutput, state: SerializationState): Unit = {
      dataOutput.writeInt(value.length)
      dataOutput.write(value)
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Array[Byte] = {
      val length = dataInput.readInt()
      val array = new Array[Byte](length)
      dataInput.readFully(array)
      array
    }
  }

  // TODO add further array types, such as for Long
}
