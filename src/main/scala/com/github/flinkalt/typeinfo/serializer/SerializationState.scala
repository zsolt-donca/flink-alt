package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.serializer.Serializer.RefId

import scala.collection.mutable
import scala.language.existentials
import scala.reflect.ClassTag

case class SerializationRefEntry[T](value: T, tag: ClassTag[_])

class SerializationState {

  private var id: RefId = 0

  private val objects: mutable.Map[SerializationRefEntry[_], RefId] = mutable.Map.empty

  private var coproductCases: Int = 0

  def put[T](value: T)(implicit tag: ClassTag[T]): RefId = {
    val newId = id + 1
    val prev = objects.put(SerializationRefEntry(value, tag), newId)
    assert(prev.isEmpty)
    id = newId
    newId
  }

  def get[T](value: T)(implicit tag: ClassTag[T]): Option[RefId] = objects.get(SerializationRefEntry(value, tag))

  def values: Iterable[Any] = objects.keys.map(_.value)

  def increaseCoproductCases(): Unit = coproductCases += 1

  def readeAndResetCoproductCases: Int = {
    val result = coproductCases
    coproductCases = 0
    result
  }
}
