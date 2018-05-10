package com.github.flinkalt.memory

import cats.Functor
import cats.data.State
import cats.instances.vector._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.github.flinkalt.time.Instant
import com.github.flinkalt.{DStream, StateTrans}

case class Data[+T](time: Instant, watermark: Instant, value: T)
object Data {
  implicit def functor: Functor[Data] = new Functor[Data] {
    override def map[A, B](fa: Data[A])(f: A => B): Data[B] = fa.copy(value = f(fa.value))
  }
}

case class MemoryDStream[+T](vector: Vector[Data[T]])

object MemoryDStream {


  implicit def memoryDStream: DStream[MemoryDStream] = new DStream[MemoryDStream] {

    override def map[A, B](fa: MemoryDStream[A])(f: A => B): MemoryDStream[B] = {
      MemoryDStream(fa.vector.map(data => data.map(f)))
    }

    override def filter[T](f: MemoryDStream[T])(predicate: T => Boolean): MemoryDStream[T] = {
      MemoryDStream(f.vector.filter(data => predicate(data.value)))
    }

    override def flatMap[T, U](f: MemoryDStream[T])(fun: T => Seq[U]): MemoryDStream[U] = {
      MemoryDStream(f.vector.flatMap(data => fun(data.value).map(u => data.as(u))))
    }

    override def collect[T, U](f: MemoryDStream[T])(pf: PartialFunction[T, U]): MemoryDStream[U] = {
      MemoryDStream(f.vector.collect {
        case data if pf.isDefinedAt(data.value) => data.as(pf.apply(data.value))
      })
    }

    override def mapWithState[K, S, T, U](f: MemoryDStream[T])(stateTrans: StateTrans[K, S, T, U]): MemoryDStream[U] = {
      val trans: Data[T] => State[Map[K, S], Data[U]] = dt => stateByKey(stateTrans.key(dt.value), stateTrans.trans(dt.value)).map(dt.as)
      val state: State[Map[K, S], Vector[Data[U]]] = f.vector.traverse(trans)
      val vector = state.runA(Map.empty).value
      MemoryDStream(vector)
    }

    override def flatMapWithState[K, S, T, U](f: MemoryDStream[T])(stateTrans: StateTrans[K, S, T, Vector[U]]): MemoryDStream[U] = {
      val trans: Data[T] => State[Map[K, S], Vector[Data[U]]] = dt => stateByKey(stateTrans.key(dt.value), stateTrans.trans(dt.value)).map(v => v.map(dt.as))
      val state: State[Map[K, S], Vector[Data[U]]] = f.vector.flatTraverse(trans)
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