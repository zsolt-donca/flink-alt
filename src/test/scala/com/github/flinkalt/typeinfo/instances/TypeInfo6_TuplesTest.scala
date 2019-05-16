package com.github.flinkalt.typeinfo.instances

import com.github.flinkalt.typeinfo.TypeInfo
import org.scalatest.PropSpec

class TypeInfo6_TuplesTest extends PropSpec with RefSerializerHelper {
  property("Serialize a pair") {
    assert(TypeInfo[(String, Int)] == TypeInfo.tuple2TypeInfo[String, Int])

    forAllRoundTrip[(String, Int)]()
  }

  property("Serialize a triplet") {
    assert(TypeInfo[(String, Int, Vector[Long])] == TypeInfo.tuple3TypeInfo[String, Int, Vector[Long]])

    forAllRoundTrip[(String, Int, Vector[Long])]()
  }
}
