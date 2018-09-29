package com.github.flinkalt

import cats.data.State
import com.github.flinkalt.typeinfo.TypeInfo
import simulacrum.typeclass

@typeclass
trait Stateful[F[_]] {
  def mapWithState[S: TypeInfo, A, B: TypeInfo](fa: F[A])(f: Stateful.StateTrans[S, A, B])(implicit keyed: Keyed[A]): F[B]
  def flatMapWithState[S: TypeInfo, A, B: TypeInfo](fa: F[A])(f: Stateful.StateTrans[S, A, Vector[B]])(implicit keyed: Keyed[A]): F[B]
}

object Stateful {
  type StateTrans[S, A, B] = A => State[Option[S], B]
}
