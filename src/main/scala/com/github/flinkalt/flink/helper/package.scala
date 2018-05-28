package com.github.flinkalt.flink

import com.github.flinkalt.Data
import com.github.flinkalt.typeinfo.TypeInfo
import org.apache.flink.streaming.api.scala.DataStream

package object helper {

  implicit class DataStreamCollectorExt[T: TypeInfo](dataStream: DataStream[T])(implicit collector: DataStreamCollector) {
    def collect(): StreamCollector[Data[T]] = {
      collector.collect[T](dataStream)
    }
  }
}
