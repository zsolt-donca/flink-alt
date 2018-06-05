package com.github.flinkalt.typeinfo.serializer

import com.github.flinkalt.typeinfo.Injection
import org.scalacheck.Arbitrary
import org.scalatest.PropSpec

import scala.util.matching.Regex

class InjectSerializerTest extends PropSpec with RefSerializerHelper {

  // apparently shapeless can derive a Generic instance for ordinary classes with constructor parameters as vals
  // in that case, our injection instance below is unnecessary (though still has priority)
  class StringPair(_s1: String, _s2: String) {
    def s1: String = _s1
    def s2: String = _s2

    def canEqual(other: Any): Boolean = other.isInstanceOf[StringPair]

    override def equals(other: Any): Boolean = other match {
      case that: StringPair =>
        (that canEqual this) &&
          s1 == that.s1 &&
          s2 == that.s2
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(s1, s2)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }

    override def toString = s"StringPair($s1, $s2)"
  }

  implicit val injectStringPairToString: Injection[StringPair, String] = new Injection[StringPair, String] {
    val pattern: Regex = "(?s)(\\d+) (\\d+) (.*)".r
    override def apply(a: StringPair): String = {
      s"${a.s1.length} ${a.s2.length} ${a.s1}${a.s2}"
    }

    override def invert(b: String): StringPair = {
      b match {
        case pattern(size1, size2, concatenated) =>
          new StringPair(concatenated.substring(0, size1.toInt), concatenated.substring(size1.toInt, size1.toInt + size2.toInt))

        case _ =>
          sys.error(s"String does not match pattern: $b")
      }
    }
  }

  implicit def arbitraryStringPair: Arbitrary[StringPair] = Arbitrary(
    for {
      s1 <- Arbitrary.arbitrary[String]
      s2 <- Arbitrary.arbitrary[String]
    } yield new StringPair(s1, s2)
  )

  property("Injection of the string pair") {
    forAllRoundTrip[StringPair]()
  }

  property("Injection of the string pair as another pair") {
    forAllRoundTripWithPair[StringPair]()
  }
}
