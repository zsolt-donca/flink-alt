package com.github.flinkalt.memory

import cats.data.State
import cats.instances.vector._
import cats.syntax.traverse._
import com.github.flinkalt.{DStream, StateTrans}

case class MemoryDStream[T](vector: Vector[T])

object MemoryDStream {
  implicit def memoryDStream: DStream[MemoryDStream] = new DStream[MemoryDStream] {

    override def map[A, B](fa: MemoryDStream[A])(f: A => B): MemoryDStream[B] = {
      MemoryDStream(fa.vector.map(f))
    }

    override def filter[T](f: MemoryDStream[T])(predicate: T => Boolean): MemoryDStream[T] = {
      MemoryDStream(f.vector.filter(predicate))
    }

    override def flatMap[T, U](f: MemoryDStream[T])(fun: T => Seq[U]): MemoryDStream[U] = {
      MemoryDStream(f.vector.flatMap(fun))
    }

    override def collect[T, U](f: MemoryDStream[T])(pf: PartialFunction[T, U]): MemoryDStream[U] = {
      MemoryDStream(f.vector.collect(pf))
    }

    override def mapWithState[K, S, T, U](f: MemoryDStream[T])(stateTrans: StateTrans[K, S, T, U]): MemoryDStream[U] = {
      val trans: T => State[Map[K, S], U] = t => stateByKey(stateTrans.key(t), stateTrans.trans(t))
      val state: State[Map[K, S], Vector[U]] = f.vector.traverse(trans)
      val vector = state.runA(Map.empty).value
      MemoryDStream(vector)
    }

    override def flatMapWithState[K, S, T, U](f: MemoryDStream[T])(stateTrans: StateTrans[K, S, T, Vector[U]]): MemoryDStream[U] = {
      val trans: T => State[Map[K, S], Vector[U]] = t => stateByKey(stateTrans.key(t), stateTrans.trans(t))
      val state: State[Map[K, S], Vector[U]] = f.vector.flatTraverse(trans)
      val vector = state.runA(Map.empty).value
      MemoryDStream(vector)
    }
  }

  def stateByKey[K, S, A](key: K, st: State[Option[S], A]): State[Map[K, S], A] = {
    val read: Map[K, S] => Option[S] = map => map.get(key)
    val write: (Map[K, S], Option[S]) => Map[K, S] = (map, os) => os.map(s => map.updated(key, s)).getOrElse(map)
    st.transformS(read, write)
  }
}