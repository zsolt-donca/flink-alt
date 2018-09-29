package com.github.flinkalt.memory

import com.github.flinkalt.api._
import com.github.flinkalt.typeinfo.TypeInfo
import shapeless.HList

object MemoryProcessing extends Processing[MemoryStream] {

  import Stateful.ops._

  override def process1[S <: HList, A, B: TypeInfo](fa: MemoryStream[A])(f: StateTrans[S, A, Vector[B]])(implicit keyed: Keyed[A], si: StateInfo[S]): MemoryStream[B] = {
    implicit def sTypeInfo: TypeInfo[S] = TypeInfo.dummy

    fa.flatMapWithState(f)
  }

  override def process2[S <: HList, A, B1: TypeInfo, B2: TypeInfo](fa: MemoryStream[A])(f: StateTrans[S, A, (Vector[B1], Vector[B2])])(implicit keyed: Keyed[A], si: StateInfo[S]): (MemoryStream[B1], MemoryStream[B2]) = {

    implicit def sTypeInfo: TypeInfo[S] = TypeInfo.dummy

    val f1 = f.andThen(_.map(_._1))
    val f2 = f.andThen(_.map(_._2))

    val res1 = fa.flatMapWithState(f1)
    val res2 = fa.flatMapWithState(f2)

    (res1, res2)
  }
}
