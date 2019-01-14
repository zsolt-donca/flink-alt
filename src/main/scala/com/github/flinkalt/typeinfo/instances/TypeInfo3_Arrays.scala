package com.github.flinkalt.typeinfo.instances

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.serializer.{DeserializationState, SerializationState, ValueSerializer}
import com.github.flinkalt.typeinfo.{DirectTypeInfo, TypeInfo}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo

trait TypeInfo3_Arrays extends TypeInfo4_Collections {
  implicit def byteArrayTypeInfo: TypeInfo[Array[Byte]] = new DirectTypeInfo(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO) with ValueSerializer[Array[Byte]] {
    override def serializeNewValue(value: Array[Byte], dataOutput: DataOutput, state: SerializationState): Unit = {
      if (value == null) {
        dataOutput.writeInt(-1)
      } else {
        dataOutput.writeInt(value.length)
        dataOutput.write(value)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Array[Byte] = {
      val length = dataInput.readInt()
      if (length == -1) {
        null.asInstanceOf[Array[Byte]]
      } else {
        val array = new Array[Byte](length)
        dataInput.readFully(array)
        array
      }
    }
  }

  // TODO add further array types, such as for Long
}
