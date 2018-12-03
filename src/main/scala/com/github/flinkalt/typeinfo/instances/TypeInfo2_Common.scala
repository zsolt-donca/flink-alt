package com.github.flinkalt.typeinfo.instances

import java.io.{DataInput, DataOutput}

import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import com.github.flinkalt.typeinfo.serializer._
import com.github.flinkalt.typeinfo.{DirectTypeInfo, SerializerBasedTypeInfo, TypeInfo}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

trait TypeInfo2_Common extends TypeInfo3_Arrays {
  implicit def stringTypeInfo: TypeInfo[String] = new DirectTypeInfo(BasicTypeInfo.STRING_TYPE_INFO) with LeafRefSerializer[String] {
    override def write(dataOutput: DataOutput, value: String): Unit = {
      val large = isStringLarge(value)
      dataOutput.writeBoolean(large)

      if (!large) {
        dataOutput.writeUTF(value)
      } else {
        val bytes = value.getBytes("utf-8")
        dataOutput.writeInt(bytes.length)
        dataOutput.write(bytes)
      }
    }

    override def read(dataInput: DataInput): String = {
      val large = dataInput.readBoolean()
      if (!large) {
        dataInput.readUTF()
      } else {
        val length = dataInput.readInt()
        val bytes = new Array[Byte](length)
        dataInput.readFully(bytes)
        new String(bytes, "utf-8")
      }
    }

    // same check as in org.apache.flink.core.memory.DataOutputSerializer.writeUTF
    private def isStringLarge(str: String): Boolean = {
      val strLength = str.length
      var utfLength = 0

      var i = 0
      while (i < strLength) {
        val c: Int = str.charAt(i)
        if ((c >= 0x0001) && (c <= 0x007F)) utfLength += 1
        else if (c > 0x07FF) utfLength += 3
        else utfLength += 2

        i += 1
      }

      utfLength > 65535
    }
  }

  implicit def optionTypeInfo[T: TypeInfo]: TypeInfo[Option[T]] = new SerializerBasedTypeInfo[Option[T]] with RefSerializer[Option[T]] {
    override val nestedTypeInfos: TypeInfo[T] = TypeInfo[T]

    override def serializeNewValue(value: Option[T], dataOutput: DataOutput, state: SerializationState): Unit = {
      value match {
        case None =>
          dataOutput.writeByte(0)
        case Some(t) =>
          dataOutput.writeByte(1)
          TypeInfo[T].serialize(t, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Option[T] = {
      val id = dataInput.readByte()
      id match {
        case 0 => None
        case 1 => Some(TypeInfo[T].deserialize(dataInput, state))
        case _ => sys.error(s"Invalid id: $id")
      }
    }
  }

  implicit def eitherTypeInfo[E: TypeInfo, T: TypeInfo]: TypeInfo[Either[E, T]] = new SerializerBasedTypeInfo[Either[E, T]]() with RefSerializer[Either[E, T]] {
    override val nestedTypeInfos: (TypeInfo[E], TypeInfo[T]) = (TypeInfo[E], TypeInfo[T])

    override def serializeNewValue(value: Either[E, T], dataOutput: DataOutput, state: SerializationState): Unit = {
      value match {
        case Left(e) =>
          dataOutput.writeByte(0)
          TypeInfo[E].serialize(e, dataOutput, state)
        case Right(t) =>
          dataOutput.writeByte(1)
          TypeInfo[T].serialize(t, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Either[E, T] = {
      val id = dataInput.readByte()
      id match {
        case 0 => Left(TypeInfo[E].deserialize(dataInput, state))
        case 1 => Right(TypeInfo[T].deserialize(dataInput, state))
        case _ => sys.error(s"Invalid id: $id")
      }
    }
  }

  implicit def validatedTypeInfo[E: TypeInfo, T: TypeInfo]: TypeInfo[Validated[E, T]] = new SerializerBasedTypeInfo[Validated[E, T]] with RefSerializer[Validated[E, T]] {
    override val nestedTypeInfos: (TypeInfo[E], TypeInfo[T]) = (TypeInfo[E], TypeInfo[T])

    override def serializeNewValue(value: Validated[E, T], dataOutput: DataOutput, state: SerializationState): Unit = {
      value match {
        case Invalid(e) =>
          dataOutput.writeByte(0)
          TypeInfo[E].serialize(e, dataOutput, state)
        case Valid(t) =>
          dataOutput.writeByte(1)
          TypeInfo[T].serialize(t, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Validated[E, T] = {
      val id = dataInput.readByte()
      id match {
        case 0 => Invalid(TypeInfo[E].deserialize(dataInput, state))
        case 1 => Valid(TypeInfo[T].deserialize(dataInput, state))
        case _ => sys.error(s"Invalid id: $id")
      }
    }
  }
}
