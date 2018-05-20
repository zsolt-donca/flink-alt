package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.serializer.Serializer.RefId

import scala.collection.mutable

class SerializationState {

  private var id: RefId = 0

  private val objects: mutable.Map[Any, RefId] = mutable.Map.empty

  private var coproductCases: Int = 0

  def put(value: Any): RefId = {
    val newId = id + 1
    val prev = objects.put(value, newId)
    assert(prev.isEmpty)
    id = newId
    newId
  }

  def get(value: Any): Option[RefId] = objects.get(value)

  def values: Iterable[Any] = objects.keys

  def increaseCoproductCases(): Unit = coproductCases += 1

  def readeAndResetCoproductCases: Int = {
    val result = coproductCases
    coproductCases = 0
    result
  }
}
