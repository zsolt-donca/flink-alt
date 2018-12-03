package com.github.flinkalt.typeinfo.serializer

import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

@Public
class UnitTypeInfo extends TypeInformation[Unit] {
  @PublicEvolving
  override def isBasicType: Boolean = true
  @PublicEvolving
  override def isTupleType: Boolean = false
  @PublicEvolving
  override def getArity: Int = 0
  @PublicEvolving
  override def getTotalFields: Int = 0
  @PublicEvolving
  override def getTypeClass: Class[Unit] = classOf[Unit]
  @PublicEvolving
  override def isKeyType: Boolean = true

  @PublicEvolving
  override def createSerializer(config: ExecutionConfig): TypeSerializer[Unit] = {
    (new UnitSerializer).asInstanceOf[TypeSerializer[Unit]]
  }

  override def canEqual(obj: scala.Any): Boolean = {
    obj.isInstanceOf[UnitTypeInfo]
  }

  override def toString = "UnitTypeInfo"

  override def equals(obj: scala.Any): Boolean = {
    obj.isInstanceOf[UnitTypeInfo]
  }

  override def hashCode(): Int = classOf[UnitTypeInfo].hashCode
}
