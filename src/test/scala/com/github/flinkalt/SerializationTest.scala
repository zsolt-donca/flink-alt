package com.github.flinkalt

import com.github.flinkalt.flink.FlinkDStream
import com.github.flinkalt.memory.{MemoryDStream, MemoryStream}
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.scalatest.FunSuite

class SerializationTest extends FunSuite {

  // methods declared here are not serializable because FunSuite isn't

  def mappingFunction(i: Int): String = i.toString
  def flatMappingFunction(i: Int): List[String] = List(i.toString)
  def filterFunction(i: Int): Boolean = i == 0
  def collectFunction: PartialFunction[Int, String] = {
    case x if filterFunction(x) => "zero"
  }

  test("Mapping something non-serializable") {
    assertThrows[InvalidProgramException] {
      MemoryDStream.map(MemoryStream.empty)(mappingFunction)
    }
    assertThrows[InvalidProgramException] {
      FlinkDStream.map(emptyIntStream)(mappingFunction)
    }
  }

  test("FlatMapping something non-serializable") {
    assertThrows[InvalidProgramException] {
      MemoryDStream.flatMap(MemoryStream.empty)(flatMappingFunction)
    }
    assertThrows[InvalidProgramException] {
      FlinkDStream.flatMap(emptyIntStream)(flatMappingFunction)
    }
  }

  test("Collecting something non-serializable") {
    assertThrows[InvalidProgramException] {
      MemoryDStream.collect(MemoryStream.empty)(collectFunction)
    }
    assertThrows[InvalidProgramException] {
      FlinkDStream.collect(emptyIntStream)(collectFunction)
    }
  }

  test("Filtering something non-serializable") {
    assertThrows[InvalidProgramException] {
      MemoryDStream.filter(MemoryStream.empty)(filterFunction)
    }
    assertThrows[InvalidProgramException] {
      FlinkDStream.filter(emptyIntStream)(filterFunction)
    }
  }

  private def emptyIntStream: DataStream[Int] = env.fromCollection(Vector.empty[Int])

  private def env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
}
