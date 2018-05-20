package com.github.flinkalt.typeinfo.serializer

import java.math.BigInteger


trait Serializer1_Primitives extends Serializer2_CommonTypes {
  implicit def booleanSerializer: Serializer[Boolean] = ValueSerializer(_.writeBoolean(_), _.readBoolean())

  implicit def byteSerializer: Serializer[Byte] = ValueSerializer(_.writeByte(_), _.readByte())

  implicit def shortSerializer: Serializer[Short] = ValueSerializer(_.writeShort(_), _.readShort())

  implicit def charSerializer: Serializer[Char] = ValueSerializer(_.writeChar(_), _.readChar())

  implicit def intSerializer: Serializer[Int] = ValueSerializer(_.writeInt(_), _.readInt())

  implicit def longSerializer: Serializer[Long] = ValueSerializer(_.writeLong(_), _.readLong())

  implicit def floatSerializer: Serializer[Float] = ValueSerializer(_.writeFloat(_), _.readFloat())

  implicit def doubleSerializer: Serializer[Double] = ValueSerializer(_.writeDouble(_), _.readDouble())

  implicit def unitSerializer: Serializer[Unit] = ValueSerializer((_, _) => {}, _ => ())

  implicit def bigIntSerializer: Serializer[BigInt] = ValueSerializer[BigInt]((out, t) => {
    val bytes = t.toByteArray
    out.writeInt(bytes.length)
    out.write(bytes)
  }, in => {
    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    BigInt(new BigInteger(bytes))
  })
}
