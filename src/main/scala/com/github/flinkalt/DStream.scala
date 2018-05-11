package com.github.flinkalt

import cats.Functor
import simulacrum.typeclass

@typeclass
trait DStream[F[_]] extends Functor[F] {
  def flatMap[A, B](f: F[A])(fun: A => Seq[B]): F[B]
  def collect[A, B](f: F[A])(pf: PartialFunction[A, B]): F[B]
  def filter[A](f: F[A])(predicate: A => Boolean): F[A]
}
