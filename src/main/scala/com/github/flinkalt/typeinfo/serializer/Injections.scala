package com.github.flinkalt.typeinfo.serializer

trait Injections[A, B] {
  def apply(a: A): B

  def invert(b: B): A
}

object Injections {
  def apply[A, B](f: A => B, g: B => A): Injections[A, B] = new Injections[A, B] {
    override def apply(a: A): B = f(a)

    override def invert(b: B): A = g(b)
  }
}
