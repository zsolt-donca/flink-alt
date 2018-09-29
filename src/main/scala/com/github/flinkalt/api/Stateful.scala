package com.github.flinkalt.api

import com.github.flinkalt.typeinfo.TypeInfo
import simulacrum.typeclass

@typeclass
trait Stateful[F[_]] {
  def mapWithState[S: TypeInfo, A, B: TypeInfo](fa: F[A])(f: StateTrans[S, A, B])(implicit keyed: Keyed[A]): F[B]
  def flatMapWithState[S: TypeInfo, A, B: TypeInfo](fa: F[A])(f: StateTrans[S, A, Vector[B]])(implicit keyed: Keyed[A]): F[B]
}

