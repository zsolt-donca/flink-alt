package com.github.flinkalt.typeinfo

import cats.syntax.invariant._
import com.github.flinkalt.typeinfo.serializer.Serializer
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import shapeless.{:+:, ::, CNil, Coproduct, Generic, HList, HNil, Lazy}

import scala.reflect.ClassTag

trait TypeInfo[T] extends Serializable {
  def serializer: Serializer[T]
  def flinkTypeInfo: TypeInformation[T]
}

object TypeInfo extends TypeInfo1_Primitives {
  def apply[T](implicit typeInfo: TypeInfo[T]): TypeInfo[T] = typeInfo

  def fromFlinkTypeInformation[T](typeInformation: TypeInformation[T])(implicit ser: Serializer[T]): TypeInfo[T] = new TypeInfo[T] {
    override def serializer: Serializer[T] = ser
    override def flinkTypeInfo: TypeInformation[T] = typeInformation
  }

  def fromSerializer[T](ser: => Serializer[T])(implicit tag: ClassTag[T]): TypeInfo[T] = new TypeInfo[T] {
    override def serializer: Serializer[T] = ser
    override def flinkTypeInfo: TypeInformation[T] = new SerializerBasedTypeInformation[T](ser, tag)
  }
}

trait TypeInfo1_Primitives extends TypeInfo2_Common {
  implicit def intTypeInfo: TypeInfo[Int] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Int]))

  implicit def longTypeInfo: TypeInfo[Long] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Long]))
}

trait TypeInfo2_Common extends TypeInfo6_Generic {
  implicit def stringTypeInfo: TypeInfo[String] = TypeInfo.fromFlinkTypeInformation(BasicTypeInfo.STRING_TYPE_INFO)
}

trait TypeInfo6_Generic {
  implicit def hnilTypeInfo: TypeInfo[HNil] = {
    TypeInfo.fromSerializer(Serializer[Unit].imap[HNil](_ => HNil)(_ => ()))
  }

  implicit def hlistTypeInfo[H, T <: HList](implicit head: Lazy[TypeInfo[H]], tail: TypeInfo[T]): TypeInfo[H :: T] = {
    TypeInfo.fromSerializer(Serializer.hlistSerializer(head.value.serializer, tail.serializer))
  }

  implicit def cnilTypeInfo: TypeInfo[CNil] = {
    TypeInfo.fromSerializer(Serializer.cnilSerializer)
  }

  implicit def coproductSerializer[H, T <: Coproduct](implicit head: Lazy[TypeInfo[H]], tail: TypeInfo[T]): TypeInfo[H :+: T] = {
    TypeInfo.fromSerializer(Serializer.coproductSerializer(head.value.serializer, tail.serializer))
  }


  implicit def genericEncoder[A: ClassTag, R](implicit gen: Generic.Aux[A, R], typeInfo: Lazy[TypeInfo[R]]): TypeInfo[A] = {
    TypeInfo.fromSerializer(Serializer.genericEncoder(gen, typeInfo.value.serializer))
  }

}
