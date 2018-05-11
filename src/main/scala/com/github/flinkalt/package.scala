package com.github

import org.apache.flink.api.common.typeinfo.TypeInformation

package object flinkalt {
  type TypeInfo[T] = TypeInformation[T]
}
