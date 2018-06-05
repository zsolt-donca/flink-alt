package com.github.flinkalt.typeinfo

trait Injection[A, B] extends Serializable {
  def apply(a: A): B

  def invert(b: B): A
}

object Injection {
  def apply[A, B](f: A => B, g: B => A): Injection[A, B] = new Injection[A, B] {
    override def apply(a: A): B = f(a)

    override def invert(b: B): A = g(b)
  }
}
