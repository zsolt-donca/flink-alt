package com.github.flinkalt.typeinfo.generic

import com.github.flinkalt.typeinfo.TypeInfo
import shapeless.Strict

package object semiauto {
  def deriveTypeInfo[T](implicit ti: Strict[MkTypeInfo[T]]): TypeInfo[T] = ti.value.value
}
