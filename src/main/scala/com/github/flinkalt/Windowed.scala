package com.github.flinkalt

import cats.kernel.Semigroup
import com.github.flinkalt.WindowTrigger.WindowTrigger
import com.github.flinkalt.time.{Duration, Instant}
import simulacrum.typeclass

@typeclass
trait Windowed[F[_]] {
  def windowReduce[K, A: Semigroup, B](fa: F[A])(windowType: WindowType, key: A => K)(trigger: WindowTrigger[K, A, B]): F[B]
}

case class Window(start: Instant, end: Instant)

sealed trait WindowType
case class TumblingWindow(size: Duration) extends WindowType
case class SlidingWindow(size: Duration, slide: Duration) extends WindowType


object WindowTrigger {
  type WindowTrigger[K, A, B] = (K, Window, A) => B

  def identity[K, A]: WindowTrigger[K, A, A] = (_, _, a) => a
}
