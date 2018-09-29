package com.github.flinkalt.api

import cats.kernel.Semigroup
import com.github.flinkalt.time.{Duration, Instant}
import com.github.flinkalt.typeinfo.TypeInfo
import simulacrum.typeclass

@typeclass
trait Windowed[F[_]] {
  def windowReduce[K: TypeInfo, A: Semigroup : TypeInfo](fa: F[A])(windowType: WindowType, key: A => K): F[A]

  def windowReduceMapped[K: TypeInfo, A: Semigroup, B: TypeInfo](fa: F[A])(windowType: WindowType, key: A => K)(trigger: WindowMapper[K, A, B]): F[B]
}

case class Window(start: Instant, end: Instant)

sealed trait WindowType

object WindowTypes {
  case class Tumbling(size: Duration) extends WindowType
  case class Sliding(size: Duration, slide: Duration) extends WindowType
}
