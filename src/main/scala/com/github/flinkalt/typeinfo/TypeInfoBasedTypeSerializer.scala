package com.github.flinkalt.typeinfo

import com.github.flinkalt.typeinfo.serializer.{DeserializationState, SerializationState}
import org.apache.flink.api.common.typeutils.{CompatibilityResult, ParameterlessTypeSerializerConfig, TypeSerializer, TypeSerializerConfigSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

final class TypeInfoBasedTypeSerializer[T](private val typeInfo: TypeInfo[T]) extends TypeSerializer[T] {

  override def duplicate(): TypeSerializer[T] = this

  override def isImmutableType: Boolean = true

  override def getLength: Int = -1

  override def copy(from: T): T = from

  override def copy(from: T, reuse: T): T = from

  override def deserialize(reuse: T, source: DataInputView): T = deserialize(source)

  override def createInstance(): T = sys.error("Not sure what this is supposed to do.")

  override def copy(source: DataInputView, target: DataOutputView): Unit = serialize(deserialize(source), target)

  override def serialize(record: T, target: DataOutputView): Unit = typeInfo.serialize(record, target, new SerializationState)

  override def deserialize(source: DataInputView): T = typeInfo.deserialize(source, new DeserializationState)

  override def snapshotConfiguration(): TypeSerializerConfigSnapshot = new ParameterlessTypeSerializerConfig("")

  override def ensureCompatibility(configSnapshot: TypeSerializerConfigSnapshot): CompatibilityResult[T] = CompatibilityResult.compatible()

  override def hashCode(): Int = typeInfo.hashCode()

  override def canEqual(obj: scala.Any): Boolean = obj.isInstanceOf[TypeInfoBasedTypeSerializer[T]]

  override def equals(o: scala.Any): Boolean = o match {
    case that: TypeInfoBasedTypeSerializer[_] =>
      (this eq that) || (that canEqual this) && this.typeInfo == that.typeInfo
    case _ => false
  }
}
