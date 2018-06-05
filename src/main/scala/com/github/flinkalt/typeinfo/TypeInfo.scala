package com.github.flinkalt.typeinfo

import com.github.flinkalt.typeinfo.serializer.Serializer
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import shapeless.{Generic, Lazy}

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.{ListMap, Seq}
import scala.collection.mutable
import scala.reflect.ClassTag

abstract class TypeInfo[T] extends Serializable {
  def tag: ClassTag[T]
  def serializer: Serializer[T]
  def flinkTypeInfo: TypeInformation[T]
}

final class DirectTypeInfo[T](private val typeInformation: TypeInformation[T], ser: Serializer[T])(implicit val tag: ClassTag[T]) extends TypeInfo[T] {
  override def serializer: Serializer[T] = ser
  override def flinkTypeInfo: TypeInformation[T] = typeInformation

  override def equals(other: Any): Boolean = {
    other match {
      case that: AnyRef if that eq this => true
      case that: DirectTypeInfo[_] =>
        typeInformation == that.typeInformation
      case _ => false
    }
  }

  override def hashCode(): Int = {
    typeInformation.hashCode()
  }
}

abstract class SerializerBasedTypeInfo[T](implicit val tag: ClassTag[T]) extends TypeInfo[T] {
  override def flinkTypeInfo: TypeInformation[T] = new SerializerBasedTypeInformation[T](this)

  def nestedTypeInfos: Seq[TypeInfo[_]]

  override def equals(other: Any): Boolean = {
    other match {
      case that: AnyRef if that eq this => true
      case that: SerializerBasedTypeInfo[_] if this.getClass == that.getClass =>
        this.tag == that.tag && this.nestedTypeInfos == that.nestedTypeInfos
      case _ => false
    }
  }

  override def hashCode(): Int = {
    tag.hashCode() * 31 + nestedTypeInfos.hashCode()
  }
}

object TypeInfo extends TypeInfo1_Primitives {
  def apply[T](implicit typeInfo: TypeInfo[T]): TypeInfo[T] = typeInfo
}

trait TypeInfo1_Primitives extends TypeInfo2_Common {
  implicit def booleanTypeInfo: TypeInfo[Boolean] = new DirectTypeInfo(getInfoFor(classOf[Boolean]), Serializer.booleanSerializer)

  implicit def byteTypeInfo: TypeInfo[Byte] = new DirectTypeInfo(getInfoFor(classOf[Byte]), Serializer.byteSerializer)

  implicit def shortTypeInfo: TypeInfo[Short] = new DirectTypeInfo(getInfoFor(classOf[Short]), Serializer.shortSerializer)

  implicit def charTypeInfo: TypeInfo[Char] = new DirectTypeInfo(getInfoFor(classOf[Char]), Serializer.charSerializer)

  implicit def intTypeInfo: TypeInfo[Int] = new DirectTypeInfo(getInfoFor(classOf[Int]), Serializer.intSerializer)

  implicit def longTypeInfo: TypeInfo[Long] = new DirectTypeInfo(getInfoFor(classOf[Long]), Serializer.longSerializer)

  implicit def floatTypeInfo: TypeInfo[Float] = new DirectTypeInfo(getInfoFor(classOf[Float]), Serializer.floatSerializer)

  implicit def doubleTypeInfo: TypeInfo[Double] = new DirectTypeInfo(getInfoFor(classOf[Double]), Serializer.doubleSerializer)

  implicit def unitTypeInfo: TypeInfo[Unit] = new DirectTypeInfo(getInfoFor(classOf[Unit]), Serializer.unitSerializer)

  implicit def bigIntTypeInfo: TypeInfo[BigInt] = new DirectTypeInfo(getInfoFor(classOf[BigInt]), Serializer.bigIntSerializer)
}

trait TypeInfo2_Common extends TypeInfo3_Arrays {
  implicit def stringTypeInfo: TypeInfo[String] = new DirectTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, Serializer.stringSerializer)

  implicit def optionTypeInfo[T: TypeInfo]: TypeInfo[Option[T]] = new SerializerBasedTypeInfo[Option[T]]() {
    override def serializer: Serializer[Option[T]] = Serializer.optionSerializer(TypeInfo[T].serializer)

    override def nestedTypeInfos: Seq[TypeInfo[_]] = Seq(TypeInfo[T])
  }
}

trait TypeInfo3_Arrays extends TypeInfo4_Collections {
  implicit def byteArrayTypeInfo: TypeInfo[Array[Byte]] = new DirectTypeInfo(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Serializer.byteArraySerializer)
}

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

  implicit def listMapSerializableCanBuildFrom[K, V]: SerializableCanBuildFrom[ListMap[K, V], (K, V), ListMap[K, V]] =
    new SerializableCanBuildFrom[ListMap[K, V], (K, V), ListMap[K, V]] {

      override def apply(from: ListMap[K, V]): mutable.Builder[(K, V), ListMap[K, V]] = from.genericBuilder[(K, V)].asInstanceOf[mutable.Builder[(K, V), ListMap[K, V]]]

      override def apply(): mutable.Builder[(K, V), ListMap[K, V]] = ListMap.newBuilder[K, V]
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

  implicit def traversableTypeInfo[C[e] <: Traversable[e], T](implicit typeInfo: TypeInfo[T], cb: SerializableCanBuildFrom[Nothing, T, C[T]], tag: ClassTag[C[T]]): TypeInfo[C[T]] = new SerializerBasedTypeInfo[C[T]] {
    override def serializer: Serializer[C[T]] = Serializer.traversableSerializer(typeInfo.serializer)

    override def nestedTypeInfos: Seq[TypeInfo[_]] = Seq(typeInfo)
  }

  implicit def mapTypeInfo[C[k, v] <: Map[k, v], K, V](implicit kti: TypeInfo[K], vti: TypeInfo[V], cb: SerializableCanBuildFrom[Nothing, (K, V), C[K, V]], tag: ClassTag[C[K, V]]): TypeInfo[C[K, V]] = new SerializerBasedTypeInfo[C[K, V]]() {
    override def serializer: Serializer[C[K, V]] = Serializer.mapSerializer(kti.serializer, vti.serializer)

    override def nestedTypeInfos: Seq[TypeInfo[_]] = Seq(kti, vti)
  }
}

trait TypeInfo5_Injections extends TypeInfo6_Generic {
  implicit def injectionTypeInfo[T: ClassTag, U](implicit inj: Injection[T, U], typeInfo: TypeInfo[U]): TypeInfo[T] = new SerializerBasedTypeInfo[T]() {
    override def serializer: Serializer[T] = Serializer.injectSerializer(inj, typeInfo.serializer)

    override def nestedTypeInfos: Seq[TypeInfo[_]] = Seq(typeInfo)
  }
}

trait TypeInfo6_Generic {
  implicit def genericEncoder[A: ClassTag, R](implicit gen: Generic.Aux[A, R], genTypeInfo: Lazy[GenTypeInfo[R]]): TypeInfo[A] = new SerializerBasedTypeInfo[A] {
    override def serializer: Serializer[A] = Serializer.genericSerializer(gen, genTypeInfo.value.typeInfo.serializer)

    override def nestedTypeInfos: Seq[TypeInfo[_]] = Seq(genTypeInfo.value.typeInfo)
  }
}
