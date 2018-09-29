package com.github.flinkalt

import cats.data.State

package object api {

  type StateTrans[S, A, B] = A => State[Option[S], B]

  type WindowMapper[K, A, B] = (K, Window, A) => B

}
