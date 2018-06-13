package com.github.flinkalt.typeinfo.serializer

import gnu.trove.map.TIntObjectMap
import gnu.trove.map.hash.TIntObjectHashMap

class DeserializationState {
  val objects: TIntObjectMap[Any] = new TIntObjectHashMap[Any]()

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
