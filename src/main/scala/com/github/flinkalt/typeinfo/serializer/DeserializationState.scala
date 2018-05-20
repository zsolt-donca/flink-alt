package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.serializer.Serializer.RefId

import scala.collection.mutable

class DeserializationState {
  private val objects: mutable.Map[RefId, Any] = mutable.Map.empty

  private var coproductCases: Int = -1

  def get(id: RefId): Option[Any] = objects.get(id)

  def put(id: RefId, value: Any): Unit = {
    val prev = objects.put(id, value)
    assert(prev.isEmpty)
  }

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
