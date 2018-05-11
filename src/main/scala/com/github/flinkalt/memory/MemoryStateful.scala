package com.github.flinkalt.memory

import cats.data.State
import cats.instances.vector._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.github.flinkalt.{StateTrans, Stateful}

object MemoryStateful extends Stateful[MemoryStream] {
  override def mapWithState[K, S, A, B](f: MemoryStream[A])(stateTrans: StateTrans[K, S, A, B]): MemoryStream[B] = {
    val trans: Data[A] => State[Map[K, S], Data[B]] = da => stateByKey(stateTrans.key(da.value), stateTrans.trans(da.value)).map(da.as)
    val vector = f.vector.traverse(trans).runA(Map.empty).value
    MemoryStream(vector)
  }

  override def flatMapWithState[K, S, A, B](f: MemoryStream[A])(stateTrans: StateTrans[K, S, A, Vector[B]]): MemoryStream[B] = {
    val trans: Data[A] => State[Map[K, S], Vector[Data[B]]] = da => stateByKey(stateTrans.key(da.value), stateTrans.trans(da.value)).map(v => v.map(da.as))
    val vector = f.vector.flatTraverse(trans).runA(Map.empty).value
    MemoryStream(vector)
  }

  private def stateByKey[K, S, A](key: K, st: State[Option[S], A]): State[Map[K, S], A] = {
    val read: Map[K, S] => Option[S] = map => map.get(key)
    val write: (Map[K, S], Option[S]) => Map[K, S] = (map, os) => os.map(s => map.updated(key, s)).getOrElse(map)
    st.transformS(read, write)
  }
}
