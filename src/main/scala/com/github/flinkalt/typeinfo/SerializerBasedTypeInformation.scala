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

import com.github.flinkalt.typeinfo.serializer.Serializer
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

import scala.reflect.ClassTag

class SerializerBasedTypeInformation[P](private val serializer: Serializer[P], private val tag: ClassTag[P])
  extends TypeInformation[P] {

  def isBasicType: Boolean = false

  def isTupleType: Boolean = false

  def isKeyType: Boolean = false

  def getArity: Int = 0

  def getTotalFields: Int = getArity

  def getTypeClass: Class[P] =
    tag.runtimeClass.asInstanceOf[Class[P]]

  def createSerializer(config: ExecutionConfig): TypeSerializer[P] = new SerializerBasedTypeSerializer[P](serializer)

  def canEqual(that: Any): Boolean =
    that.isInstanceOf[SerializerBasedTypeInformation[_]]

  override def equals(other: Any): Boolean = other match {
    case that: SerializerBasedTypeInformation[_] =>
      (this eq that) || (that canEqual this) && this.serializer == that.serializer
    case _ => false
  }

  override def hashCode: Int =
    serializer.##

  override def toString: String = s"${getTypeClass.getTypeName}"
}
