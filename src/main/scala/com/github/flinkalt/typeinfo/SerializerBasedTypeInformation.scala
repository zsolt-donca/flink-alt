/*
 * Copyright 2017 Georgi Krastev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.flinkalt.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

class SerializerBasedTypeInformation[P](private val typeInfo: TypeInfo[P])
  extends TypeInformation[P] {

  def isBasicType: Boolean = false

  def isTupleType: Boolean = false

  def isKeyType: Boolean = false

  def getArity: Int = 0

  def getTotalFields: Int = getArity

  def getTypeClass: Class[P] =
    typeInfo.tag.runtimeClass.asInstanceOf[Class[P]]

  def createSerializer(config: ExecutionConfig): TypeSerializer[P] = new SerializerBasedTypeSerializer[P](typeInfo.serializer)

  def canEqual(that: Any): Boolean =
    that.isInstanceOf[SerializerBasedTypeInformation[_]]

  override def equals(other: Any): Boolean = other match {
    case that: SerializerBasedTypeInformation[_] =>
      (that canEqual this) &&
        typeInfo == that.typeInfo
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(typeInfo)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"TypeInfo[${getTypeClass.getTypeName}]"
}
