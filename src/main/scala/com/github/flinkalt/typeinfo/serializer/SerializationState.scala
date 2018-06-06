package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.serializer.Serializer.RefId
import gnu.trove.map.TObjectIntMap

import scala.language.existentials
import scala.reflect.ClassTag

case class SerializationRefEntry[T](value: T, tag: ClassTag[_])

class SerializationState {

  var id: RefId = 0

  val objects: java.util.HashMap[Class[_], TObjectIntMap[Any]] = new java.util.HashMap

  var coproductCases: Int = 0
}
