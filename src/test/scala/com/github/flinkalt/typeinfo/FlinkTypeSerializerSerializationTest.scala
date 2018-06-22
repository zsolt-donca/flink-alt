package com.github.flinkalt.typeinfo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerConfigSnapshot, TypeSerializerSerializationUtil}
import org.apache.flink.api.java.tuple.{Tuple2 => FTuple2}
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class FlinkTypeSerializerSerializationTest extends FunSuite {
  test("Generic type serializers can be serialized by Flink") {
    val typeInfo = SerializerHelper.createTypeInfoForGenericTypeInObject()

    assertRoundTripOfSerializerAndConfig(typeInfo)
  }

  private def assertRoundTripOfSerializerAndConfig(typeInfo: TypeInformation[_]): Unit = {
    val serializer = typeInfo.createSerializer(null)
    val config = serializer.snapshotConfiguration()

    val bos = new ByteArrayOutputStream()
    val out = new DataOutputViewStreamWrapper(bos)
    //noinspection ScalaRedundantCast
    val serializersAndConfigs = List(FTuple2.of(serializer, config)).asJava.asInstanceOf[util.List[FTuple2[TypeSerializer[_], TypeSerializerConfigSnapshot]]]
    TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(out, serializersAndConfigs)

    val in = new DataInputViewStreamWrapper(new ByteArrayInputStream(bos.toByteArray))
    val results = TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, getClass.getClassLoader).asScala

    assert(results.size == 1)
    val serializerCopy = results.head.f0
    val configCopy = results.head.f1

    assert(serializer == serializerCopy)
    assert(config == configCopy)
  }
}

case class CaseClass(s: String, i: Int)

object SerializerHelper extends Serializable {
  def createTypeInfoForGenericTypeInObject(): TypeInformation[_] = implicitly[TypeInformation[CaseClass]]
}