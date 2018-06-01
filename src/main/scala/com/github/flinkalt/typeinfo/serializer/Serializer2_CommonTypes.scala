package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

trait Serializer2_CommonTypes extends Serializer3_Arrays {
  def stringSerializer: Serializer[String] = RefSerializer(_.writeUTF(_), _.readUTF())

  def optionSerializer[T](ser: Serializer[T]): Serializer[Option[T]] = new RefSerializer[Option[T]] {
    override def serializeNewValue(value: Option[T], dataOutput: DataOutput, state: SerializationState): Unit = {
      value match {
        case None =>
          dataOutput.writeByte(0)
        case Some(t) =>
          dataOutput.writeByte(1)
          ser.serialize(t, dataOutput, state)
      }
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): Option[T] = {
      val id = dataInput.readByte()
      id match {
        case 0 => None
        case 1 => Some(ser.deserialize(dataInput, state))
        case _ => sys.error(s"Invalid id: $id")
      }
    }
  }
}
