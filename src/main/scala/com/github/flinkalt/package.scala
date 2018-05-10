package com.github.flinkalt

import cats.Functor
import cats.data.State
import cats.kernel.Semigroup
import com.github.flinkalt.time.{Duration, Instant}
import simulacrum.typeclass

@typeclass
trait DStream[F[_]] extends Functor[F] {
  def flatMap[A, B](f: F[A])(fun: A => Seq[B]): F[B]
  def collect[A, B](f: F[A])(pf: PartialFunction[A, B]): F[B]
  def filter[A](f: F[A])(predicate: A => Boolean): F[A]

  def mapWithState[K, S, A, B](f: F[A])(stateTrans: StateTrans[K, S, A, B]): F[B]
  def flatMapWithState[K, S, A, B](f: F[A])(stateTrans: StateTrans[K, S, A, Vector[B]]): F[B]

  def windowReduce[K, A: Semigroup, B](fa: F[A])(windowType: WindowType, windowReduce: WindowReduce[K, A, B]): F[B]
}

case class StateTrans[+K, S, -A, B](key: A => K, trans: A => State[Option[S], B])


case class Window(start: Instant, end: Instant)

sealed trait WindowType
case class TumblingWindow(size: Duration) extends WindowType
case class SlidingWindow(size: Duration, slide: Duration) extends WindowType

case class WindowReduce[K, A, B](key: A => K, trigger: (K, Window, A) => B)

object WindowReduce {
  def apply[K, A](key: A => K): WindowReduce[K, A, A] = WindowReduce(key, (_, _, a) => a)
}
