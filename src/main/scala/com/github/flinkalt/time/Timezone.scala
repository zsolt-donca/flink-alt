package com.github.flinkalt.time

import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.generic.semiauto
import io.circe.{Decoder, Encoder, HCursor, Json}

case class Timezone(id: String) extends AnyVal

object Timezone {
  val UTC = Timezone("UTC")

  implicit def timezoneTypeInfo: TypeInfo[Timezone] = semiauto.deriveTypeInfo

  implicit def timezoneEncoder: Encoder[Timezone] = (timezone: Timezone) => Json.fromString(timezone.id)

  implicit def timezoneDecoder: Decoder[Timezone] = (c: HCursor) => Decoder.decodeString(c).map(Timezone(_))
}
