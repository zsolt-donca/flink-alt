package com.github.flinkalt

import cats.Functor
import com.github.flinkalt.time.Instant
import com.github.flinkalt.typeinfo.TypeInfo
import simulacrum.typeclass

case class Data[+T](time: Instant, watermark: Instant, value: T)

object Data {
  implicit def functor: Functor[Data] = new Functor[Data] {
    override def map[A, B](fa: Data[A])(f: A => B): Data[B] = fa.copy(value = f(fa.value))
  }
}


@typeclass
trait DStream[F[_]] {
  def map[A, B: TypeInfo](fa: F[A])(f: A => B): F[B]
  def flatMap[A, B: TypeInfo](f: F[A])(fun: A => Seq[B]): F[B]
  def collect[A, B: TypeInfo](f: F[A])(pf: PartialFunction[A, B]): F[B]
  def filter[A](f: F[A])(predicate: A => Boolean): F[A]
}
