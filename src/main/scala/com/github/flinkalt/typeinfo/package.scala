package com.github.flinkalt

import org.apache.flink.api.common.typeinfo.TypeInformation

package object typeinfo {

  object auto {
    // Shadow the default macro based TypeInformation providers.
    def createTypeInformation: Nothing = ???
    def createTuple2TypeInformation: Nothing = ???

    implicit def toFlinkTypeInformation[T](implicit typeInfo: TypeInfo[T]): TypeInformation[T] = typeInfo.flinkTypeInfo
  }

}
