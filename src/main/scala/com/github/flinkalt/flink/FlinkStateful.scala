package com.github.flinkalt.flink

import com.github.flinkalt.{StateTrans, Stateful, TypeInfo}
import org.apache.flink.streaming.api.scala.DataStream

object FlinkStateful extends Stateful[DataStream] {
  override def mapWithState[K: TypeInfo, S: TypeInfo, A, B: TypeInfo](dataStream: DataStream[A])(stateTrans: StateTrans[K, S, A, B]): DataStream[B] = {
    dataStream
      .keyBy(stateTrans.key)
      .mapWithState[B, S]((a, maybeState) => stateTrans.trans(a).run(maybeState).value.swap)
  }

  override def flatMapWithState[K: TypeInfo, S: TypeInfo, A, B: TypeInfo](dataStream: DataStream[A])(stateTrans: StateTrans[K, S, A, Vector[B]]): DataStream[B] = {
    dataStream
      .keyBy(stateTrans.key)
      .flatMapWithState[B, S]((a, maybeState) => stateTrans.trans(a).run(maybeState).value.swap)
  }
}
