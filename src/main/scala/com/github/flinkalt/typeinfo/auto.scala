package com.github.flinkalt.typeinfo

import com.github.flinkalt.typeinfo.instances.TypeInfoInstances
import org.apache.flink.api.common.typeinfo.TypeInformation

trait AutomaticTypeInformationDerivation extends TypeInfoInstances {
  // Shadow the default macro based TypeInformation providers.
  def createTypeInformation: Nothing = sys.error("This shadows Flink's built-in type info generation.")
  def createTuple2TypeInformation: Nothing = sys.error("This shadows Flink's built-in type info generation.")

  implicit def toFlinkTypeInformation[T](implicit typeInfo: TypeInfo[T]): TypeInformation[T] = typeInfo.flinkTypeInfo
}

object auto extends AutomaticTypeInformationDerivation
