package com.github.flinkalt.typeinfo.serializer

import java.io.{DataInput, DataOutput}

import scala.collection.generic.CanBuild
import scala.language.higherKinds

trait Serializer4_Collections extends Serializer5_Injections {

  implicit def traversableSerializer[C[e] <: Traversable[e], T](implicit cb: CanBuild[T, C[T]], ser: Serializer[T]): Serializer[C[T]] = new RefSerializer[C[T]] {
    override def serializeNewValue(value: C[T], dataOutput: DataOutput, state: SerializationState): Unit = {
      dataOutput.writeInt(value.size)
      value.foreach(t => ser.serialize(t, dataOutput, state))
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): C[T] = {
      val b = cb()
      val size = dataInput.readInt()
      b.sizeHint(size)
      for (_ <- 1 to size) {
        val t = ser.deserialize(dataInput, state)
        b += t
      }
      b.result()
    }
  }

  implicit def mapSerializer[C[k, v] <: Map[k, v], K, V](implicit cb: CanBuild[(K, V), C[K, V]], ks: Serializer[K], vs: Serializer[V]): Serializer[C[K, V]] = new RefSerializer[C[K, V]] {
    override def serializeNewValue(value: C[K, V], dataOutput: DataOutput, state: SerializationState): Unit = {
      dataOutput.writeInt(value.size)
      value.foreach({
        case (k, v) =>
          ks.serialize(k, dataOutput, state)
          vs.serialize(v, dataOutput, state)
      })
    }

    override def deserializeNewValue(dataInput: DataInput, state: DeserializationState): C[K, V] = {
      val b = cb()
      val size = dataInput.readInt()
      b.sizeHint(size)
      for (i <- 1 to size) {
        val k = ks.deserialize(dataInput, state)
        val v = vs.deserialize(dataInput, state)
        b += Tuple2(k, v)
      }
      b.result()
    }
  }
}
