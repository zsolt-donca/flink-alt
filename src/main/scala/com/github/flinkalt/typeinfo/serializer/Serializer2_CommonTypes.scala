package com.github.flinkalt.typeinfo.serializer

trait Serializer2_CommonTypes extends Serializer3_Arrays {
  implicit def stringSerializer: Serializer[String] = RefSerializer(_.writeUTF(_), _.readUTF())
}
