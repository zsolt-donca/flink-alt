package com.github.flinkalt.typeinfo.serializer

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

@Internal
@SerialVersionUID(5413377487955047394L)
class UnitSerializer extends TypeSerializerSingleton[Unit] {

  def isImmutableType: Boolean = true

  def createInstance(): Unit = ()

  def copy(from: Unit): Unit = ()

  def copy(from: Unit, reuse: Unit): Unit = ()

  def getLength: Int = 1

  def serialize(record: Unit, target: DataOutputView): Unit = {
  }

  def deserialize(source: DataInputView): Unit = {
    ()
  }

  def deserialize(reuse: Unit, source: DataInputView): Unit = {
    ()
  }

  def copy(source: DataInputView, target: DataOutputView): Unit = {
  }

  override def hashCode(): Int = classOf[UnitSerializer].hashCode

  override def canEqual(obj: scala.Any): Boolean = {
    obj.isInstanceOf[UnitSerializer]
  }
}
