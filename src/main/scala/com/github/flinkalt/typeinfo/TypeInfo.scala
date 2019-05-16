package com.github.flinkalt.typeinfo

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.instances._
import com.github.flinkalt.typeinfo.serializer._
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.reflect.ClassTag

abstract class TypeInfo[T] extends Serializer[T] with Serializable {
  def tag: ClassTag[T]
  def flinkTypeInfo: TypeInformation[T]
}

abstract class DirectTypeInfo[T](typeInformation: TypeInformation[T])(implicit val tag: ClassTag[T]) extends TypeInfo[T] {
  override def flinkTypeInfo: TypeInformation[T] = typeInformation

  override def equals(other: Any): Boolean = {
    other match {
      case that: AnyRef if that eq this => true
      case that: DirectTypeInfo[_]      =>
        tag == that.tag
      case _                            => false
    }
  }

  override def hashCode(): Int = {
    tag.hashCode()
  }
}

abstract class SerializerBasedTypeInfo[T](implicit val tag: ClassTag[T]) extends TypeInfo[T] {
  override def flinkTypeInfo: TypeInformation[T] = new TypeInfoBasedTypeInformation[T](this)

  def nestedTypeInfos: Any

  override def equals(other: Any): Boolean = {
    other match {
      case that: AnyRef if that eq this                                       => true
      case that: SerializerBasedTypeInfo[_] if this.getClass == that.getClass =>
        this.tag == that.tag && this.nestedTypeInfos == that.nestedTypeInfos
      case _                                                                  => false
    }
  }

  override lazy val hashCode: Int = {
    tag.hashCode() * 31 + nestedTypeInfos.hashCode()
  }
}

object TypeInfo
  extends TypeInfo1_Primitives
    with TypeInfo2_Common
    with TypeInfo3_Arrays
    with TypeInfo4_Collections
    with TypeInfo5_Injections
    with TypeInfo6_Tuples {
  @inline def apply[T](implicit typeInfo: TypeInfo[T]): TypeInfo[T] = typeInfo

  def dummy[T]: TypeInfo[T] = new TypeInfo[T] {

    private def dummy: Nothing = sys.error("This is a dummy implementation. Use it only where you require a TypeInfo but are absolutely sure that it's not required (e.g. with MemoryStream)")

    override def tag: ClassTag[T] = dummy
    override def flinkTypeInfo: TypeInformation[T] = dummy
    override def serialize(value: T, dataOutput: DataOutput, state: SerializationState): Unit = dummy
    override def deserialize(dataInput: DataInput, state: DeserializationState): T = dummy
    override def serializeNewValue(value: T, dataOutput: DataOutput, state: SerializationState): Unit = dummy
    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): T = dummy
  }
}
