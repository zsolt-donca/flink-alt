package com.github.flinkalt

import com.github.flinkalt.typeinfo.TypeInfo

trait Keyed[A] {
  type K

  def fun: A => K

  implicit def typeInfo: TypeInfo[K]
}

object Keyed {
  //noinspection ConvertExpressionToSAM,TypeAnnotation
  def create[A, Key: TypeInfo](f: A => Key) = new Keyed[A] {
    type K = Key

    override def fun: A => K = f

    override def typeInfo: TypeInfo[K] = TypeInfo[K]
  }

  def apply[A](implicit keyed: Keyed[A]): Keyed[A] = keyed
}
