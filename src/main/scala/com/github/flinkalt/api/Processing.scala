package com.github.flinkalt.api

import com.github.flinkalt.typeinfo.TypeInfo
import shapeless._
import simulacrum.typeclass

@typeclass
trait Processing[F[_]] {
  def process1[S <: HList, A, B: TypeInfo](fa: F[A])(f: StateTrans[S, A, Vector[B]])(implicit keyed: Keyed[A], si: StateInfo[S]): F[B]
  def process2[S <: HList, A, B1: TypeInfo, B2: TypeInfo](fa: F[A])(f: StateTrans[S, A, (Vector[B1], Vector[B2])])(implicit keyed: Keyed[A], si: StateInfo[S]): (F[B1], F[B2])
}

sealed trait StateInfo[T]

case class HConsStateInfo[H, T <: HList](hti: TypeInfo[H], tsi: StateInfo[T]) extends StateInfo[H :: T]

case object HNilStateInfo extends StateInfo[HNil]

//noinspection TypeAnnotation
object StateInfo {
  implicit def hConsStateInfo[H, T <: HList](implicit hti: TypeInfo[H], tsi: StateInfo[T]) = HConsStateInfo[H, T](hti, tsi)

  implicit def hnilStateInfo = HNilStateInfo
}