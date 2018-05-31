package com.github.flinkalt.typeinfo

import cats.syntax.invariant._
import com.github.flinkalt.typeinfo.serializer.Serializer
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import shapeless.{:+:, ::, CNil, Coproduct, Generic, HList, HNil, Lazy}

import scala.collection.generic.CanBuild
import scala.reflect.ClassTag

abstract class TypeInfo[T] extends Serializable {
  def serializer: Serializer[T]
  def flinkTypeInfo: TypeInformation[T]
}

abstract class SerializerBasedTypeInfo[T](implicit tag: ClassTag[T]) extends TypeInfo[T] {
  override def flinkTypeInfo: TypeInformation[T] = new SerializerBasedTypeInformation[T](serializer, tag)
}

object TypeInfo extends TypeInfo1_Primitives {
  def apply[T](implicit typeInfo: TypeInfo[T]): TypeInfo[T] = typeInfo

  def fromFlinkTypeInformation[T](typeInformation: TypeInformation[T], ser: Serializer[T]): TypeInfo[T] = new TypeInfo[T] {
    override def serializer: Serializer[T] = ser
    override def flinkTypeInfo: TypeInformation[T] = typeInformation
  }
}

trait TypeInfo1_Primitives extends TypeInfo2_Common {
  implicit def booleanTypeInfo: TypeInfo[Boolean] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Boolean]), Serializer.booleanSerializer)

  implicit def byteTypeInfo: TypeInfo[Byte] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Byte]), Serializer.byteSerializer)

  implicit def shortTypeInfo: TypeInfo[Short] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Short]), Serializer.shortSerializer)

  implicit def charTypeInfo: TypeInfo[Char] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Char]), Serializer.charSerializer)

  implicit def intTypeInfo: TypeInfo[Int] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Int]), Serializer.intSerializer)

  implicit def longTypeInfo: TypeInfo[Long] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Long]), Serializer.longSerializer)

  implicit def floatTypeInfo: TypeInfo[Float] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Float]), Serializer.floatSerializer)

  implicit def doubleTypeInfo: TypeInfo[Double] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Double]), Serializer.doubleSerializer)

  implicit def unitTypeInfo: TypeInfo[Unit] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[Unit]), Serializer.unitSerializer)

  implicit def bigIntTypeInfo: TypeInfo[BigInt] = TypeInfo.fromFlinkTypeInformation(getInfoFor(classOf[BigInt]), Serializer.bigIntSerializer)
}

trait TypeInfo2_Common extends TypeInfo3_Arrays {
  implicit def stringTypeInfo: TypeInfo[String] = TypeInfo.fromFlinkTypeInformation(BasicTypeInfo.STRING_TYPE_INFO, Serializer.stringSerializer)
}

trait TypeInfo3_Arrays extends TypeInfo4_Collections {
  implicit def byteArrayTypeInfo: TypeInfo[Array[Byte]] = TypeInfo.fromFlinkTypeInformation(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Serializer.byteArraySerializer)
}

trait TypeInfo4_Collections extends TypeInfo5_Injections {
  implicit def traversableTypeInfo[C[e] <: Traversable[e], T](implicit typeInfo: TypeInfo[T], cb: CanBuild[T, C[T]], tag: ClassTag[C[T]]): TypeInfo[C[T]] = new SerializerBasedTypeInfo[C[T]] {
    override def serializer: Serializer[C[T]] = Serializer.traversableSerializer(typeInfo.serializer)
  }

  implicit def mapTypeInfo[C[k, v] <: Map[k, v], K, V](implicit kti: TypeInfo[K], vti: TypeInfo[V], cb: CanBuild[(K, V), C[K, V]], tag: ClassTag[C[K, V]]): TypeInfo[C[K, V]] = new SerializerBasedTypeInfo[C[K, V]]() {
    override def serializer: Serializer[C[K, V]] = Serializer.mapSerializer(kti.serializer, vti.serializer)
  }
}

trait TypeInfo5_Injections extends TypeInfo6_Generic {
  implicit def injectionTypeInfo[T: ClassTag, U](inj: Injection[T, U], typeInfo: TypeInfo[U]): TypeInfo[T] = new SerializerBasedTypeInfo[T]() {
    override def serializer: Serializer[T] = Serializer.injectSerializer(inj, typeInfo.serializer)
  }
}

trait TypeInfo6_Generic {
  implicit def hnilTypeInfo: TypeInfo[HNil] = new SerializerBasedTypeInfo[HNil] {
    override def serializer: Serializer[HNil] = Serializer.unitSerializer.imap[HNil](_ => HNil)(_ => ())
  }

  implicit def hlistTypeInfo[H, T <: HList](implicit head: Lazy[TypeInfo[H]], tail: TypeInfo[T]): TypeInfo[H :: T] = new SerializerBasedTypeInfo[H :: T] {
    override def serializer: Serializer[H :: T] = Serializer.hlistSerializer(head.value.serializer, tail.serializer)
  }

  implicit def cnilTypeInfo: TypeInfo[CNil] = new SerializerBasedTypeInfo[CNil] {
    override def serializer: Serializer[CNil] = Serializer.cnilSerializer
  }

  implicit def coproductSerializer[H, T <: Coproduct](implicit head: Lazy[TypeInfo[H]], tail: TypeInfo[T]): TypeInfo[H :+: T] = new SerializerBasedTypeInfo[H :+: T] {
    override def serializer: Serializer[H :+: T] = Serializer.coproductSerializer(head.value.serializer, tail.serializer)
  }

  implicit def genericEncoder[A: ClassTag, R](implicit gen: Generic.Aux[A, R], typeInfo: Lazy[TypeInfo[R]]): TypeInfo[A] = new SerializerBasedTypeInfo[A] {
    override def serializer: Serializer[A] = Serializer.genericSerializer(gen, typeInfo.value.serializer)
  }
}
