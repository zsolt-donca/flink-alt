package com.github.flinkalt.typeinfo

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
      case that: DirectTypeInfo[_] =>
        tag == that.tag
      case _ => false
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
      case that: AnyRef if that eq this => true
      case that: SerializerBasedTypeInfo[_] if this.getClass == that.getClass =>
        this.tag == that.tag && this.nestedTypeInfos == that.nestedTypeInfos
      case _ => false
    }
  }

  override lazy val hashCode: Int = {
    tag.hashCode() * 31 + nestedTypeInfos.hashCode()
  }
}

object TypeInfo {
  @inline def apply[T](implicit typeInfo: TypeInfo[T]): TypeInfo[T] = typeInfo
}
