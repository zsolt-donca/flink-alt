package com.github.flinkalt.typeinfo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.github.flinkalt.typeinfo.AutoFlinkTypeInfo._
import com.github.flinkalt.typeinfo.generic.auto._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.scalatest.FunSuite

class FlinkTypeSerializerSerializationTest extends FunSuite {

  test("Generic type serializers can be serialized by Flink") {
    val typeInfo = SerializerHelper.createTypeInfoForGenericTypeInObject()

    assertRoundTripOfSerializerAndConfig(typeInfo)
  }

  test("Unit type serializers can serialize data") {
    val typeInfo = implicitly[TypeInformation[Unit]]

    val data: Unit = ()

    val bos = new ByteArrayOutputStream()
    val out = new DataOutputViewStreamWrapper(bos)
    val serializer = typeInfo.createSerializer(null)
    serializer.serialize(data, out)

    val in = new DataInputViewStreamWrapper(new ByteArrayInputStream(bos.toByteArray))
    serializer.deserialize(in)
  }

  private def assertRoundTripOfSerializerAndConfig(typeInfo: TypeInformation[_]): Unit = {
    val serializer = typeInfo.createSerializer(null)

    val bos = new ByteArrayOutputStream()
    val out = new DataOutputViewStreamWrapper(bos)

    val typeSerializerSnapshot = TypeSerializerUtils.snapshotBackwardsCompatible(serializer).head
    val version = typeSerializerSnapshot.getCurrentVersion

    typeSerializerSnapshot.writeSnapshot(out)

    val in = new DataInputViewStreamWrapper(new ByteArrayInputStream(bos.toByteArray))
    typeSerializerSnapshot.readSnapshot(version, in, getClass.getClassLoader)

    assert(serializer == typeSerializerSnapshot.restoreSerializer())
  }
}

case class CaseClass(s: String, i: Int)

object SerializerHelper extends Serializable {
  def createTypeInfoForGenericTypeInObject(): TypeInformation[_] = implicitly[TypeInformation[CaseClass]]
}