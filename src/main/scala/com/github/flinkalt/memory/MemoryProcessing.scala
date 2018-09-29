package com.github.flinkalt.memory

import com.github.flinkalt.api._
import com.github.flinkalt.typeinfo.TypeInfo
import shapeless.HList

object MemoryProcessing extends Processing[MemoryStream] {

  import DStream.ops._
  import Stateful.ops._

  override def process1[S <: HList, A, B: TypeInfo](fa: MemoryStream[A])(f: StateTrans[S, A, B])(implicit keyed: Keyed[A], si: StateInfo[S]): MemoryStream[B] = {
    implicit def sTypeInfo: TypeInfo[S] = TypeInfo.dummy

    fa.mapWithState(f)
  }

  override def process2[S <: HList, A, B1: TypeInfo, B2: TypeInfo](fa: MemoryStream[A])(f: StateTrans[S, A, (B1, B2)])(implicit keyed: Keyed[A], si: StateInfo[S]): (MemoryStream[B1], MemoryStream[B2]) = {

    implicit def sTypeInfo: TypeInfo[S] = TypeInfo.dummy

    implicit def b1b2TypeInfo: TypeInfo[(B1, B2)] = TypeInfo.dummy

    val res = fa.mapWithState(f)

    (res.map(_._1), res.map(_._2))
  }
}
