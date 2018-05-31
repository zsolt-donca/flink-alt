package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}
import java.math.BigInteger


trait Serializer1_Primitives extends Serializer2_CommonTypes {
  def booleanSerializer: Serializer[Boolean] = ValueSerializer(_.writeBoolean(_), _.readBoolean())

  def byteSerializer: Serializer[Byte] = ValueSerializer(_.writeByte(_), _.readByte())

  def shortSerializer: Serializer[Short] = ValueSerializer(_.writeShort(_), _.readShort())

  def charSerializer: Serializer[Char] = ValueSerializer(_.writeChar(_), _.readChar())

  def intSerializer: Serializer[Int] = ValueSerializer(_.writeInt(_), _.readInt())

  def longSerializer: Serializer[Long] = ValueSerializer(_.writeLong(_), _.readLong())

  def floatSerializer: Serializer[Float] = ValueSerializer(_.writeFloat(_), _.readFloat())

  def doubleSerializer: Serializer[Double] = ValueSerializer(_.writeDouble(_), _.readDouble())

  def unitSerializer: Serializer[Unit] = ValueSerializer((_, _) => {}, _ => ())

  def bigIntSerializer: Serializer[BigInt] = new ValueSerializer[BigInt] {
    override def serializeNewValue(t: BigInt, out: DataOutput, state: SerializationState): Unit = {
      val bytes = t.toByteArray
      out.writeInt(bytes.length)
      out.write(bytes)
    }

    override def deserializeNewValue(in: DataInput, state: DeserializationState): BigInt = {
      val length = in.readInt()
      val bytes = new Array[Byte](length)
      in.readFully(bytes)
      BigInt(new BigInteger(bytes))
    }
  }
}
