package com.github.flinkalt.flink

import com.github.flinkalt.memory.DataAndWatermark
import com.github.flinkalt.typeinfo.TypeInfo
import org.apache.flink.streaming.api.scala.DataStream

package object helper {

  implicit class DataStreamCollectorExt[T: TypeInfo](dataStream: DataStream[T])(implicit collector: DataStreamCollector) {
    def collect(): StreamCollector[DataAndWatermark[T]] = {
      collector.collect[T](dataStream)
    }
  }
}
