package com.github.flinkalt

import com.github.flinkalt.api.{DStream, Stateful, Windowed}
import org.apache.flink.streaming.api.scala.DataStream

package object flink {
  implicit def flinkDStream: DStream[DataStream] = FlinkDStream
  implicit def flinkStateful: Stateful[DataStream] = FlinkStateful
  implicit def flinkWindowed: Windowed[DataStream] = FlinkWindowed
}
