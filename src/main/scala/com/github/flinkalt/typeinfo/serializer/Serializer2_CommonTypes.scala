package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}

import scala.reflect.ClassTag

trait Serializer2_CommonTypes extends Serializer3_Arrays {
  def stringSerializer: Serializer[String] = RefSerializer(_.writeUTF(_), _.readUTF())

  def optionSerializer[T: ClassTag](ser: Serializer[T]): Serializer[Option[T]] = new RefSerializer[Option[T]] {
    override def serializeNewValue(value: Option[T], dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[Option[T]]): Unit = {
      value match {
        case None =>
          dataOutput.writeByte(0)
        case Some(t) =>
          dataOutput.writeByte(1)
          ser.serialize(t, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[Option[T]]): Option[T] = {
      val id = dataInput.readByte()
      id match {
        case 0 => None
        case 1 => Some(ser.deserialize(dataInput, state))
        case _ => sys.error(s"Invalid id: $id")
      }
    }
  }

  def eitherSerializer[E: ClassTag, T: ClassTag](es: Serializer[E], ts: Serializer[T]): Serializer[Either[E, T]] = new RefSerializer[Either[E, T]] {
    override def serializeNewValue(value: Either[E, T], dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[Either[E, T]]): Unit = {
      value match {
        case Left(e) =>
          dataOutput.writeByte(0)
          es.serialize(e, dataOutput, state)
        case Right(t) =>
          dataOutput.writeByte(1)
          ts.serialize(t, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[Either[E, T]]): Either[E, T] = {
      val id = dataInput.readByte()
      id match {
        case 0 => Left(es.deserialize(dataInput, state))
        case 1 => Right(ts.deserialize(dataInput, state))
        case _ => sys.error(s"Invalid id: $id")
      }
    }
  }

  def validatedSerializer[E: ClassTag, T: ClassTag](es: Serializer[E], ts: Serializer[T]): Serializer[Validated[E, T]] = new RefSerializer[Validated[E, T]] {
    override def serializeNewValue(value: Validated[E, T], dataOutput: DataOutput, state: SerializationState)(implicit tag: ClassTag[Validated[E, T]]): Unit = {
      value match {
        case Invalid(e) =>
          dataOutput.writeByte(0)
          es.serialize(e, dataOutput, state)
        case Valid(t) =>
          dataOutput.writeByte(1)
          ts.serialize(t, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState)(implicit tag: ClassTag[Validated[E, T]]): Validated[E, T] = {
      val id = dataInput.readByte()
      id match {
        case 0 => Invalid(es.deserialize(dataInput, state))
        case 1 => Valid(ts.deserialize(dataInput, state))
        case _ => sys.error(s"Invalid id: $id")
      }
    }
  }
}
