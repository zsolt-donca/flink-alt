package com.github.flinkalt.typeinfo.instances

import org.scalatest.PropSpec

class TypeInfo3_ArraysTest extends PropSpec with RefSerializerHelper {
  property("Arrays of bytes are serialized") {
    forAllRoundTrip[Array[Byte]]()
  }
}
