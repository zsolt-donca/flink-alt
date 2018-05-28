package com.github.flinkalt.flink

import com.github.flinkalt.DStream
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.streaming.api.scala.DataStream

object FlinkDStream extends DStream[DataStream] {
  override def map[A, B: TypeInfo](fa: DataStream[A])(f: A => B): DataStream[B] = fa.map(f)

  override def flatMap[A, B: TypeInfo](f: DataStream[A])(fun: A => Seq[B]): DataStream[B] = f.flatMap(fun)

  override def collect[A, B: TypeInfo](f: DataStream[A])(pf: PartialFunction[A, B]): DataStream[B] = f.flatMap(a => pf.lift(a))

  override def filter[A](f: DataStream[A])(predicate: A => Boolean): DataStream[A] = f.filter(predicate)
}
