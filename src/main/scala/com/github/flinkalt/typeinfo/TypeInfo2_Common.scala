package com.github.flinkalt.typeinfo

import java.io.{DataInput, DataOutput}

import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import com.github.flinkalt.typeinfo.serializer._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

trait TypeInfo2_Common extends TypeInfo3_Arrays {
  implicit def stringTypeInfo: TypeInfo[String] = new DirectTypeInfo(BasicTypeInfo.STRING_TYPE_INFO) with LeafRefSerializer[String] {
    override def write(dataOutput: DataOutput, value: String): Unit = dataOutput.writeUTF(value)

    override def read(dataInput: DataInput): String = dataInput.readUTF()
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
