package com.github.flinkalt.typeinfo

import java.io.{DataInput, DataOutput}

import com.github.flinkalt.typeinfo.serializer.{DeserializationState, RefSerializer, SerializationState}

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag

// workaround to make the collection-related type information instances serializable
trait SerializableCanBuildFrom[-From, -Elem, +To] extends CanBuildFrom[From, Elem, To] with Serializable

object SerializableCanBuildFrom extends SerializableCanBuildFrom_Lower {
  implicit def listSerializableCanBuildFrom[T]: SerializableCanBuildFrom[List[T], T, List[T]] =
    new SerializableCanBuildFrom[List[T], T, List[T]] {
      override def apply(from: List[T]): mutable.Builder[T, List[T]] = from.genericBuilder[T]

      override def apply(): mutable.Builder[T, List[T]] = List.newBuilder[T]
    }

  implicit def vectorSerializableCanBuildFrom[T]: SerializableCanBuildFrom[Vector[T], T, Vector[T]] =
    new SerializableCanBuildFrom[Vector[T], T, Vector[T]] {
      override def apply(from: Vector[T]): mutable.Builder[T, Vector[T]] = from.genericBuilder[T]

      override def apply(): mutable.Builder[T, Vector[T]] = Vector.newBuilder[T]
    }
}

trait SerializableCanBuildFrom_Lower {
  implicit def mapSerializableCanBuildFrom[K, V]: SerializableCanBuildFrom[Map[K, V], (K, V), Map[K, V]] =
    new SerializableCanBuildFrom[Map[K, V], (K, V), Map[K, V]] {

      override def apply(from: Map[K, V]): mutable.Builder[(K, V), Map[K, V]] = from.genericBuilder[(K, V)].asInstanceOf[mutable.Builder[(K, V), Map[K, V]]]

      override def apply(): mutable.Builder[(K, V), Map[K, V]] = Map.newBuilder[K, V]
    }
}

trait TypeInfo4_Collections extends TypeInfo5_Injections {

  implicit def traversableTypeInfo[C[e] <: Traversable[e], T](implicit typeInfo: TypeInfo[T], cb: SerializableCanBuildFrom[Nothing, T, C[T]], ct: ClassTag[C[T]]): TypeInfo[C[T]] = new SerializerBasedTypeInfo[C[T]] with RefSerializer[C[T]] {
    override val nestedTypeInfos: TypeInfo[T] = typeInfo

    override def serializeNewValue(value: C[T], dataOutput: DataOutput, state: SerializationState): Unit = {
      dataOutput.writeInt(value.size)
      value.foreach(t => typeInfo.serialize(t, dataOutput, state))
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): C[T] = {
      val b = cb()
      val size = dataInput.readInt()
      b.sizeHint(size)
      for (_ <- 1 to size) {
        val t = typeInfo.deserialize(dataInput, state)
        b += t
      }
      b.result()
    }
  }

  implicit def mapTypeInfo[C[k, v] <: Map[k, v], K, V](implicit kti: TypeInfo[K], vti: TypeInfo[V], cb: SerializableCanBuildFrom[Nothing, (K, V), C[K, V]], ct: ClassTag[C[K, V]]): TypeInfo[C[K, V]] = new SerializerBasedTypeInfo[C[K, V]] with RefSerializer[C[K, V]] {
    override val nestedTypeInfos: (TypeInfo[K], TypeInfo[V]) = (kti, vti)

    override def serializeNewValue(value: C[K, V], dataOutput: DataOutput, state: SerializationState): Unit = {
      dataOutput.writeInt(value.size)
      value.foreach({
        case (k, v) =>
          kti.serialize(k, dataOutput, state)
          vti.serialize(v, dataOutput, state)
      })
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): C[K, V] = {
      val b = cb()
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
}
