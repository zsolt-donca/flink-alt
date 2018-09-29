package com.github.flinkalt.flink

import com.github.flinkalt.api._
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.Collector
import shapeless._

import scala.annotation.tailrec

//noinspection ConvertExpressionToSAM,ScalaRedundantCast
object FlinkProcessing extends Processing[DataStream] {
  override def process1[S <: HList, A, B: TypeInfo](dataStream: DataStream[A])(f: StateTrans[S, A, B])(implicit keyed: Keyed[A], si: StateInfo[S]): DataStream[B] = {
    import keyed.typeInfo
    dataStream
      .keyBy(keyed.fun)
      .process(new ProcessFunction[A, B] {

        private var stateDescriptors: Map[StateInfo[_], ValueStateDescriptor[_]] = Map.empty

        override def open(parameters: Configuration): Unit = {
          stateDescriptors = buildStateDescriptorMap(si, 0, Map.empty)
        }

        override def processElement(value: A, ctx: ProcessFunction[A, B]#Context, out: Collector[B]): Unit = {

          val maybeState = readState(si).asInstanceOf[Option[S]]

          val (maybeNewState, result) = f(value).run(maybeState).value

          maybeNewState match {
            case Some(newState) => updateState(si, newState)
            case None => deleteState(si)
          }

          out.collect(result)
        }

        @tailrec
        private def buildStateDescriptorMap(si: StateInfo[_], index: Int, out: Map[StateInfo[_], ValueStateDescriptor[_]]): Map[StateInfo[_], ValueStateDescriptor[_]] = {
          si match {
            case HConsStateInfo(hti, tsi) =>
              val vsd = new ValueStateDescriptor(s"state-$index", hti.flinkTypeInfo)
              buildStateDescriptorMap(tsi, index + 1, out + (si -> vsd))

            case HNilStateInfo =>
              out
          }
        }

        private def readState(si: StateInfo[_]): Option[HList] = {
          si match {
            case HConsStateInfo(_, tsi) =>
              val vsd = stateDescriptors(si)
              val head = getRuntimeContext.getState(vsd).value()
              if (head != null) {
                readState(tsi).map(tail => head :: tail)
              } else {
                None
              }
            case HNilStateInfo =>
              Some(HNil)
          }
        }

        @tailrec
        private def updateState(si: StateInfo[_], state: HList): Unit = {
          si match {
            case HConsStateInfo(_, tsi) =>
              val head :: tail = state.asInstanceOf[::[_, _ <: HList]]
              getRuntimeContext.getState(stateDescriptors(si)).asInstanceOf[ValueState[Any]].update(head)
              updateState(tsi, tail)

            case HNilStateInfo =>
          }
        }

        @tailrec
        private def deleteState(si: StateInfo[_]): Unit = {
          si match {
            case HConsStateInfo(_, tsi) =>
              getRuntimeContext.getState(stateDescriptors(si)).asInstanceOf[ValueState[Any]].update(null)
              deleteState(tsi)

            case HNilStateInfo =>
          }
        }
      })
  }

  override def process2[S <: HList, A, B1: TypeInfo, B2: TypeInfo](fa: DataStream[A])(f: StateTrans[S, A, (B1, B2)])(implicit keyed: Keyed[A], si: StateInfo[S]): (DataStream[B1], DataStream[B2]) = ???
}
