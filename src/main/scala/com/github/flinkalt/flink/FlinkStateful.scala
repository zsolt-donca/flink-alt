package com.github.flinkalt.flink

import com.github.flinkalt.api.{Keyed, StateTrans, Stateful}
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.streaming.api.scala.DataStream

object FlinkStateful extends Stateful[DataStream] {
  override def mapWithState[S: TypeInfo, A, B: TypeInfo](dataStream: DataStream[A])(stateTrans: StateTrans[S, A, B])(implicit keyed: Keyed[A]): DataStream[B] = {
    import keyed.typeInfo
    dataStream
      .keyBy(keyed.fun)
      .mapWithState[B, S]((a, maybeState) => stateTrans(a).run(maybeState).value.swap)
  }

  override def flatMapWithState[S: TypeInfo, A, B: TypeInfo](dataStream: DataStream[A])(stateTrans: StateTrans[S, A, Vector[B]])(implicit keyed: Keyed[A]): DataStream[B] = {
    import keyed.typeInfo
    dataStream
      .keyBy(keyed.fun)
      .flatMapWithState[B, S]((a, maybeState) => stateTrans(a).run(maybeState).value.swap)
  }
}
