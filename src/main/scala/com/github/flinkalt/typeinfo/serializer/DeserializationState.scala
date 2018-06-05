package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.serializer.Serializer.RefId

import scala.collection.mutable
import scala.language.existentials
import scala.reflect.ClassTag

case class DeserializationRefEntry(id: RefId, tag: ClassTag[_])

class DeserializationState {
  private val objects: mutable.Map[DeserializationRefEntry, Any] = mutable.Map.empty

  private var coproductCases: Int = -1

  def get[T](id: RefId)(implicit tag: ClassTag[T]): Option[T] = {
    objects.get(DeserializationRefEntry(id, tag)).asInstanceOf[Option[T]]
  }

  def put[T](id: RefId, value: T)(implicit tag: ClassTag[T]): Unit = {
    val prev = objects.put(DeserializationRefEntry(id, tag), value)
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
