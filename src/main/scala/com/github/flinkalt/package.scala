package com.github.flinkalt

import cats.Functor
import cats.data.State
import cats.kernel.Semigroup
import com.github.flinkalt.time.Duration
import simulacrum.typeclass

@typeclass
trait DStream[F[_]] extends Functor[F] {
  def flatMap[T, U](f: F[T])(fun: T => Seq[U]): F[U]
  def collect[T, U](f: F[T])(pf: PartialFunction[T, U]): F[U]
  def filter[T](f: F[T])(predicate: T => Boolean): F[T]

  def mapWithState[K, S, T, U](f: F[T])(stateTrans: StateTrans[K, S, T, U]): F[U]
  def flatMapWithState[K, S, T, U](f: F[T])(stateTrans: StateTrans[K, S, T, Vector[U]]): F[U]


}

case class StateTrans[+K, S, -A, B](key: A => K, trans: A => State[Option[S], B])

object StateTrans {

}

sealed trait WindowType
case class TumblingWindow(size: Duration) extends WindowType
case class SlidingWindow(size: Duration, slide: Duration) extends WindowType

trait MapReduce[K, T, M] {
  def key: T => K

  def mapping(implicit M: Semigroup[M]): T => M
}
