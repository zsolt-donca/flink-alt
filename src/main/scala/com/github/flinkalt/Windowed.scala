package com.github.flinkalt

import cats.kernel.Semigroup
import com.github.flinkalt.Windowed.WindowMapper
import com.github.flinkalt.time.{Duration, Instant}
import simulacrum.typeclass

@typeclass
trait Windowed[F[_]] {
  def windowReduce[K: TypeInfo, A: Semigroup : TypeInfo](fa: F[A])(windowType: WindowType, key: A => K): F[A]

  def windowReduceMapped[K: TypeInfo, A: Semigroup, B: TypeInfo](fa: F[A])(windowType: WindowType, key: A => K)(trigger: WindowMapper[K, A, B]): F[B]
}

object Windowed {
  type WindowMapper[K, A, B] = (K, Window, A) => B
}

case class Window(start: Instant, end: Instant)

sealed trait WindowType
case class TumblingWindow(size: Duration) extends WindowType
case class SlidingWindow(size: Duration, slide: Duration) extends WindowType
