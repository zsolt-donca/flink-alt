package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.serializer.SerializationState.RefId
import gnu.trove.map.TObjectIntMap

import scala.reflect.ClassTag

case class SerializationRefEntry[T](value: T, tag: ClassTag[_])

class SerializationState {

  var lastId: RefId = 0

  val objects: java.util.HashMap[Serializer[_], TObjectIntMap[Any]] = new java.util.HashMap

  var coproductCases: Int = 0
}

object SerializationState {
  type RefId = Int
}