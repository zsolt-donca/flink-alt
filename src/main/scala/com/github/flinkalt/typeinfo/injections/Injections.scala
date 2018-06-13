package com.github.flinkalt.typeinfo.injections

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.github.flinkalt.typeinfo.Injection

import scala.reflect.ClassTag

object Injections {
  def byteArrayInjection[T <: java.io.Serializable : ClassTag]: Injection[T, Array[Byte]] = Injection(toByteArray[T], fromByteArray[T])

  private def toByteArray[T <: java.io.Serializable](value: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(value)
    oos.close()
    bos.toByteArray
  }

  private def fromByteArray[T <: java.io.Serializable : ClassTag](array: Array[Byte]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(array))
    val o = ois.readObject()
    o match {
      case t: T => t
      case _ => sys.error(s"Something else came out of serialization: object = $o, class = ${implicitly[ClassTag[T]]}")
    }
  }
}
