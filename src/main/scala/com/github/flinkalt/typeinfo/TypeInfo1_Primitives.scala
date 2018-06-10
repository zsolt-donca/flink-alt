package com.github.flinkalt.typeinfo

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.serializer.{DeserializationState, SerializationState, ValueSerializer}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor

trait TypeInfo1_Primitives extends TypeInfo2_Common {
  implicit def booleanTypeInfo: TypeInfo[Boolean] = new DirectTypeInfo(getInfoFor(classOf[Boolean])) with ValueSerializer[Boolean] {
    override def serializeNewValue(value: Boolean, dataOutput: DataOutput, state: SerializationState): Unit = dataOutput.writeBoolean(value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Boolean = dataInput.readBoolean()
  }

  implicit def byteTypeInfo: TypeInfo[Byte] = new DirectTypeInfo(getInfoFor(classOf[Byte])) with ValueSerializer[Byte] {
    override def serializeNewValue(value: Byte, dataOutput: DataOutput, state: SerializationState): Unit = dataOutput.writeByte(value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Byte = dataInput.readByte()
  }

  implicit def shortTypeInfo: TypeInfo[Short] = new DirectTypeInfo(getInfoFor(classOf[Short])) with ValueSerializer[Short] {
    override def serializeNewValue(value: Short, dataOutput: DataOutput, state: SerializationState): Unit = dataOutput.writeShort(value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Short = dataInput.readShort()
  }

  implicit def charTypeInfo: TypeInfo[Char] = new DirectTypeInfo(getInfoFor(classOf[Char])) with ValueSerializer[Char] {
    override def serializeNewValue(value: Char, dataOutput: DataOutput, state: SerializationState): Unit = dataOutput.writeChar(value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Char = dataInput.readChar()
  }

  implicit def intTypeInfo: TypeInfo[Int] = new DirectTypeInfo(getInfoFor(classOf[Int])) with ValueSerializer[Int] {
    override def serializeNewValue(value: Int, dataOutput: DataOutput, state: SerializationState): Unit = dataOutput.writeInt(value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Int = dataInput.readInt()
  }

  implicit def longTypeInfo: TypeInfo[Long] = new DirectTypeInfo(getInfoFor(classOf[Long])) with ValueSerializer[Long] {
    override def serializeNewValue(value: Long, dataOutput: DataOutput, state: SerializationState): Unit = dataOutput.writeLong(value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Long = dataInput.readLong()
  }

  implicit def floatTypeInfo: TypeInfo[Float] = new DirectTypeInfo(getInfoFor(classOf[Float])) with ValueSerializer[Float] {
    override def serializeNewValue(value: Float, dataOutput: DataOutput, state: SerializationState): Unit = dataOutput.writeFloat(value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Float = dataInput.readFloat()
  }

  implicit def doubleTypeInfo: TypeInfo[Double] = new DirectTypeInfo(getInfoFor(classOf[Double])) with ValueSerializer[Double] {
    override def serializeNewValue(value: Double, dataOutput: DataOutput, state: SerializationState): Unit = dataOutput.writeDouble(value)

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Double = dataInput.readDouble()
  }

  implicit def unitTypeInfo: TypeInfo[Unit] = new DirectTypeInfo(getInfoFor(classOf[Unit])) with ValueSerializer[Unit] {
    override def serializeNewValue(value: Unit, dataOutput: DataOutput, state: SerializationState): Unit = {}

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Unit = ()
  }
}
