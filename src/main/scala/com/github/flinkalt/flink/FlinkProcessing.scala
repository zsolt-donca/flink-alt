package com.github.flinkalt.flink

import com.github.flinkalt.api._
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.util.Collector
import shapeless._

import scala.annotation.tailrec

//noinspection ConvertExpressionToSAM,ScalaRedundantCast
object FlinkProcessing extends Processing[DataStream] {
  override def process1[S <: HList, A, B: TypeInfo](dataStream: DataStream[A])(f: StateTrans[S, A, B])(implicit keyed: Keyed[A], si: StateInfo[S]): DataStream[B] = {
    import keyed.typeInfo
    dataStream
      .keyBy(keyed.fun)
      .process(new StateProcessing[S, A, B](si) {
        override def processElement(value: A, ctx: ProcessFunction[A, B]#Context, out: Collector[B]): Unit = {
          val result = runWithState(value, f)

          out.collect(result)
        }
      })
  }

  override def process2[S <: HList, A, B1: TypeInfo, B2: TypeInfo](dataStream: DataStream[A])(f: StateTrans[S, A, (B1, B2)])(implicit keyed: Keyed[A], si: StateInfo[S]): (DataStream[B1], DataStream[B2]) = {
    import keyed.typeInfo

    val b2OutTag = OutputTag[B2]("out-2")

    val b1Stream = dataStream
      .keyBy(keyed.fun)
      .process(new StateProcessing[S, A, B1](si) {
        override def processElement(value: A, ctx: ProcessFunction[A, B1]#Context, out: Collector[B1]): Unit = {
          val (b1, b2) = runWithState(value, f)

          out.collect(b1)
          ctx.output(b2OutTag, b2)
        }
      })

    val b2Stream = b1Stream.getSideOutput(b2OutTag)

    (b1Stream, b2Stream)
  }
}

//noinspection ScalaRedundantCast
abstract class StateProcessing[S <: HList, A, Out](si: StateInfo[S]) extends ProcessFunction[A, Out] {
  var stateDescriptors: Map[StateInfo[_], ValueStateDescriptor[_]] = _

  override def open(parameters: Configuration): Unit = {
    stateDescriptors = buildStateDescriptorMap(si)
  }

  def buildStateDescriptorMap(si: StateInfo[_]): Map[StateInfo[_], ValueStateDescriptor[_]] = {
    buildStateDescriptorMap(si, 0, Map.empty)
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

  def runWithState[B](value: A, f: StateTrans[S, A, B]): B = {
    val maybeState = readState(si).asInstanceOf[Option[S]]

    val (maybeNewState, result) = f(value).run(maybeState).value

    maybeNewState match {
      case Some(newState) => updateState(si, newState)
      case None => deleteState(si)
    }

    result
  }

  def readState(si: StateInfo[_]): Option[HList] = {
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
  final def updateState(si: StateInfo[_], state: HList): Unit = {
    si match {
      case HConsStateInfo(_, tsi) =>
        val head :: tail = state.asInstanceOf[::[_, _ <: HList]]
        getRuntimeContext.getState(stateDescriptors(si)).asInstanceOf[ValueState[Any]].update(head)
        updateState(tsi, tail)

      case HNilStateInfo =>
    }
  }

  @tailrec
  final def deleteState(si: StateInfo[_]): Unit = {
    si match {
      case HConsStateInfo(_, tsi) =>
        getRuntimeContext.getState(stateDescriptors(si)).asInstanceOf[ValueState[Any]].update(null)
        deleteState(tsi)

      case HNilStateInfo =>
    }
  }
}
