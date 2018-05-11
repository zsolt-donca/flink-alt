package com.github.flinkalt

import cats.data.State
import simulacrum.typeclass

@typeclass
trait Stateful[F[_]] {
  def mapWithState[K: TypeInfo, S: TypeInfo, A, B: TypeInfo](f: F[A])(stateTrans: StateTrans[K, S, A, B]): F[B]
  def flatMapWithState[K: TypeInfo, S: TypeInfo, A, B: TypeInfo](f: F[A])(stateTrans: StateTrans[K, S, A, Vector[B]]): F[B]
}

case class StateTrans[K, S, A, B](key: A => K, trans: A => State[Option[S], B])
