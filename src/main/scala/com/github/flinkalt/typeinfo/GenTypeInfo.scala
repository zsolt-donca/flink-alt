package com.github.flinkalt.typeinfo

import cats.syntax.invariant._
import com.github.flinkalt.typeinfo.serializer.Serializer
import shapeless.{:+:, ::, CNil, Coproduct, HList, HNil, Lazy}

trait GenTypeInfo[T] extends Serializable {
  def typeInfo: TypeInfo[T]
}

object GenTypeInfo {
  implicit def hnilTypeInfo: GenTypeInfo[HNil] = new GenTypeInfo[HNil] {
    override def typeInfo: TypeInfo[HNil] = new SerializerBasedTypeInfo[HNil] {
      override def serializer: Serializer[HNil] = Serializer.unitSerializer.imap[HNil](_ => HNil)(_ => ())
    }
  }

  implicit def hlistTypeInfo[H, T <: HList](implicit head: Lazy[TypeInfo[H]], tail: GenTypeInfo[T]): GenTypeInfo[H :: T] = new GenTypeInfo[::[H, T]] {
    override def typeInfo: TypeInfo[H :: T] = new SerializerBasedTypeInfo[H :: T] {
      override def serializer: Serializer[H :: T] = Serializer.hlistSerializer(head.value.serializer, tail.typeInfo.serializer)
    }
  }

  implicit def cnilTypeInfo: GenTypeInfo[CNil] = new GenTypeInfo[CNil] {
    override def typeInfo: TypeInfo[CNil] = new SerializerBasedTypeInfo[CNil] {
      override def serializer: Serializer[CNil] = Serializer.cnilSerializer
    }
  }

  implicit def coproductSerializer[H, T <: Coproduct](implicit head: Lazy[TypeInfo[H]], tail: GenTypeInfo[T]): GenTypeInfo[H :+: T] = new GenTypeInfo[H :+: T] {
    override def typeInfo: TypeInfo[H :+: T] = new SerializerBasedTypeInfo[H :+: T] {
      override def serializer: Serializer[H :+: T] = Serializer.coproductSerializer(head.value.serializer, tail.typeInfo.serializer)
    }
  }
}