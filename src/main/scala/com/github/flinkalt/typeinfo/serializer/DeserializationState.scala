package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.serializer.Serializer.RefId
import gnu.trove.map.TIntObjectMap

import scala.language.existentials
import scala.reflect.ClassTag

case class DeserializationRefEntry(id: RefId, tag: ClassTag[_])

class DeserializationState {
  val objects: java.util.HashMap[Class[_], TIntObjectMap[Any]] = new java.util.HashMap

  private var coproductCases: Int = -1

  def withoutCoproductCases: Boolean = coproductCases < 0

  def setCoproductCases(value: Int): Unit = {
    assert(coproductCases == -1)
    coproductCases = value
  }

  def decreaseAndCheckCoproductCases: Boolean = {
    assert(coproductCases >= 0)
    coproductCases -= 1
    val result = coproductCases < 0
    result
  }
}
