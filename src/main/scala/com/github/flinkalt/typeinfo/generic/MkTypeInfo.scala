package com.github.flinkalt.typeinfo.generic

import cats.data.Validated
import com.github.flinkalt.typeinfo.TypeInfo

case class MkTypeInfo[T](value: TypeInfo[T])

object MkTypeInfo extends TypeInfo10_Generic with UseMkTypeInfo {
  def apply[T](implicit mk: MkTypeInfo[T]): MkTypeInfo[T] = mk

  implicit def optionMkTypeInfo[T: TypeInfo]: MkTypeInfo[Option[T]] = MkTypeInfo(TypeInfo[Option[T]])

  implicit def eitherMkTypeInfo[E: TypeInfo, T: TypeInfo]: MkTypeInfo[Either[E, T]] = MkTypeInfo(TypeInfo[Either[E, T]])

  implicit def validatedMkTypeInfo[E: TypeInfo, T: TypeInfo]: MkTypeInfo[Validated[E, T]] = MkTypeInfo(TypeInfo[Validated[E, T]])

  implicit def listMkTypeInfo[T: TypeInfo]: MkTypeInfo[List[T]] = MkTypeInfo(TypeInfo[List[T]])

  implicit def vectorMkTypeInfo[T: TypeInfo]: MkTypeInfo[Vector[T]] = MkTypeInfo(TypeInfo[Vector[T]])
}

trait UseMkTypeInfo {

  implicit def optionMkMkTypeInfo[T: MkTypeInfo]: MkTypeInfo[Option[T]] = MkTypeInfo(TypeInfo.optionTypeInfo(MkTypeInfo[T].value))

  implicit def eitherMkMkTypeInfo[E: MkTypeInfo, T: MkTypeInfo]: MkTypeInfo[Either[E, T]] = MkTypeInfo(TypeInfo.eitherTypeInfo(MkTypeInfo[E].value, MkTypeInfo[T].value))

  implicit def validatedMkMkTypeInfo[E: MkTypeInfo, T: MkTypeInfo]: MkTypeInfo[Validated[E, T]] = MkTypeInfo(TypeInfo.validatedTypeInfo(MkTypeInfo[E].value, MkTypeInfo[T].value))

  implicit def listMkMkTypeInfo[T: MkTypeInfo]: MkTypeInfo[List[T]] = MkTypeInfo(TypeInfo.listTypeInfo(MkTypeInfo[T].value))

  implicit def vectorMkMkTypeInfo[T: MkTypeInfo]: MkTypeInfo[Vector[T]] = MkTypeInfo(TypeInfo.vectorTypeInfo(MkTypeInfo[T].value))

}