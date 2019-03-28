package com.github.flinkalt.typeinfo.instances

import cats.data.{NonEmptyList, Validated}
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.generic.auto._
import org.scalatest.PropSpec

class TypeInfo2_CommonTest extends PropSpec with RefSerializerHelper {
  property("Equal strings should be same when deserialized") {
    forAllRoundTripWithPair[String]()
  }

  // the ones below could be derived as Generic as well, but we prefer our hand-rolled one (for performance reasons)

  property("Serialize an Option") {
    assert(TypeInfo[Option[String]] == TypeInfo.optionTypeInfo[String])

    forAllRoundTrip[Option[String]]()
  }

  property("Serialize an either") {
    assert(TypeInfo[Either[String, Either[Int, Long]]] == TypeInfo.eitherTypeInfo[String, Either[Int, Long]])

    forAllRoundTrip[Either[String, Either[Int, Long]]]()
  }

  property("Serialize a validated") {
    assert(TypeInfo[Validated[NonEmptyList[String], Long]] == TypeInfo.validatedTypeInfo[NonEmptyList[String], Long])

    forAllRoundTrip[Validated[NonEmptyList[String], Long]]()
  }

  // inspired by https://www.drillio.com/en/2009/java-encoded-string-too-long-64kb-limit/
  property("Can serialize strings larger than 64 kb") {
    val sb = new StringBuilder
    for (_ <- 0 to 10000) {
      sb.append("1234567890")
    }
    val largeString = sb.toString

    roundTrip(largeString)
  }
}
