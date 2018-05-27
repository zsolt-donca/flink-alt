package com.github.flinkalt.typeinfo.desc

import com.github.flinkalt.typeinfo.serializer.Injection

sealed trait TypeDesc[+T]

object TypeDesc extends TypeDesc1_Primitives {
  def apply[T](implicit T: TypeDesc[T]): TypeDesc[T] = T
}

case object BooleanTypeDesc extends TypeDesc[Boolean]
case object ByteTypeDesc extends TypeDesc[Byte]
case object ShortTypeDesc extends TypeDesc[Short]
case object CharTypeDesc extends TypeDesc[Char]
case object IntTypeDesc extends TypeDesc[Int]
case object LongTypeDesc extends TypeDesc[Long]
case object FloatTypeDesc extends TypeDesc[Float]
case object DoubleTypeDesc extends TypeDesc[Double]
case object UnitTypeDesc extends TypeDesc[Unit]
case object BigIntTypeDesc extends TypeDesc[BigInt]

trait TypeDesc1_Primitives extends TypeDesc2_CommonTypes {
  implicit def booleanType: BooleanTypeDesc.type = BooleanTypeDesc
  implicit def byteType: ByteTypeDesc.type = ByteTypeDesc
  implicit def shortType: ShortTypeDesc.type = ShortTypeDesc
  implicit def charType: CharTypeDesc.type = CharTypeDesc
  implicit def intType: IntTypeDesc.type = IntTypeDesc
  implicit def longType: LongTypeDesc.type = LongTypeDesc
  implicit def floatType: FloatTypeDesc.type = FloatTypeDesc
  implicit def doubleType: DoubleTypeDesc.type = DoubleTypeDesc
  implicit def unitType: UnitTypeDesc.type = UnitTypeDesc
  implicit def bigIntType: BigIntTypeDesc.type = BigIntTypeDesc
}


case object StringTypeDesc extends TypeDesc[String]

trait TypeDesc2_CommonTypes extends TypeDesc3_Arrays {
  implicit def stringType: StringTypeDesc.type = StringTypeDesc
}

case object ByteArrayTypeDesc extends TypeDesc[Array[Byte]]
trait TypeDesc3_Arrays extends TypeDesc4_Collections {
  implicit def byteArrayType: ByteArrayTypeDesc.type = ByteArrayTypeDesc
}


case class TraversableType[C[_] <: Traversable[_], T](typeDesc: TypeDesc[T]) extends TypeDesc[C[T]]
case class MapType[C[_, _] <: Map[_, _], K, V](keyTypeDesc: TypeDesc[K], valueTypeDesc: TypeDesc[V]) extends TypeDesc[C[K, V]]

trait TypeDesc4_Collections extends TypeDesc5_Injections {
  implicit def traversableTypeDesc[C[e] <: Traversable[e], T](implicit t: TypeDesc[T]): TraversableType[C, T] = TraversableType[C, T](t)
  implicit def mapTypeDesc[C[_, _] <: Map[_, _], K, V](implicit k: TypeDesc[K], v: TypeDesc[V]): MapType[C, K, V] = MapType[C, K, V](k, v)
}


case class InjectionTypeDesc[T, U](inj: Injection[T, U], uTypeDesc: TypeDesc[U]) extends TypeDesc[T]

trait TypeDesc5_Injections extends TypeDesc6_Generic {
  implicit def injectionTypeDesc[T, U](implicit inj: Injection[T, U], uTypeDesc: TypeDesc[U]): InjectionTypeDesc[T, U] = InjectionTypeDesc[T, U](inj, uTypeDesc)
}


case class GenericTypeDesc[T]() extends TypeDesc[T]

trait TypeDesc6_Generic {
  implicit def genericTypeDesc[T]: GenericTypeDesc[T] = GenericTypeDesc()
}
