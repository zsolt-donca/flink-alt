package com.github.flinkalt.api

import com.github.flinkalt.typeinfo.TypeInfo
import simulacrum.typeclass

@typeclass
trait Keyed[A] {
  type K

  def fun: A => K

  implicit def typeInfo: TypeInfo[K]
}

//noinspection ConvertExpressionToSAM,TypeAnnotation
object Keyed {
  def create[A, Key: TypeInfo](f: A => Key) = new Keyed[A] {
    type K = Key

    override def fun: A => K = f

    override def typeInfo: TypeInfo[K] = TypeInfo[K]
  }
}
