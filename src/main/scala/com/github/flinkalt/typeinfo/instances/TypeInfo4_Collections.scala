package com.github.flinkalt.typeinfo.instances

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.serializer.{DeserializationState, RefSerializer, SerializationState}
import com.github.flinkalt.typeinfo.{SerializerBasedTypeInfo, TypeInfo}

import scala.collection.generic.CanBuild
import scala.reflect.ClassTag

abstract class CollectionTypeInfo[C[e] <: Traversable[e], T](implicit typeInfo: TypeInfo[T], ct: ClassTag[C[T]])
  extends SerializerBasedTypeInfo[C[T]] with RefSerializer[C[T]] {

  def canBuild: CanBuild[T, C[T]]

  final override val nestedTypeInfos: TypeInfo[T] = typeInfo

  final override def serializeNewValue(value: C[T], dataOutput: DataOutput, state: SerializationState): Unit = {
    dataOutput.writeInt(value.size)
    value.foreach((t: T) => typeInfo.serialize(t, dataOutput, state))
  }

  final override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): C[T] = {
    val b = canBuild()
    val size = dataInput.readInt()
    b.sizeHint(size)
    for (_ <- 1 to size) {
      val t = typeInfo.deserialize(dataInput, state)
      b += t
    }
    b.result()
  }
}

abstract class MapTypeInfo[C[k, v] <: Map[k, v], K, V](implicit kti: TypeInfo[K], vti: TypeInfo[V], ct: ClassTag[C[K, V]])
  extends SerializerBasedTypeInfo[C[K, V]] with RefSerializer[C[K, V]] {

  def canBuild: CanBuild[(K, V), C[K, V]]

  final override val nestedTypeInfos: (TypeInfo[K], TypeInfo[V]) = (kti, vti)

  final override def serializeNewValue(value: C[K, V], dataOutput: DataOutput, state: SerializationState): Unit = {
    dataOutput.writeInt(value.size)
    value.foreach({
      case (k, v) =>
        kti.serialize(k, dataOutput, state)
        vti.serialize(v, dataOutput, state)
    })
  }

  final override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): C[K, V] = {
    val b = canBuild()
    val size = dataInput.readInt()
    b.sizeHint(size)
    for (i <- 1 to size) {
      val k = kti.deserialize(dataInput, state)
      val v = vti.deserialize(dataInput, state)
      b += Tuple2(k, v)
    }
    b.result()
  }
}

trait TypeInfo4_Collections extends TypeInfo5_Injections {
  implicit def listTypeInfo[T: TypeInfo]: CollectionTypeInfo[List, T] = new CollectionTypeInfo[List, T]() {
    override def canBuild: CanBuild[T, List[T]] = List.canBuildFrom[T]
  }

  implicit def vectorTypeInfo[T: TypeInfo]: CollectionTypeInfo[Vector, T] = new CollectionTypeInfo[Vector, T]() {
    override def canBuild: CanBuild[T, Vector[T]] = Vector.canBuildFrom[T]
  }

  implicit def maTypeInfo[K: TypeInfo, V: TypeInfo]: MapTypeInfo[Map, K, V] = new MapTypeInfo[Map, K, V]() {
    override def canBuild: CanBuild[(K, V), Map[K, V]] = Map.canBuildFrom[K, V]
  }
}
