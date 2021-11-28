package org.ronruby.lib.csv

import cats.Show
import cats.implicits._
import org.scalacheck.Prop._
import org.scalacheck.{Gen, Prop}
import org.scalatestplus.scalacheck.Checkers

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.UUID
import scala.util.Try

class ParserSpec extends TestSpec with Checkers {

  private trait TestSetup {
    val minValue = 10
    val maxValue = 100
    implicit def dummyParser: Parser[Dummy] = (s: String) => Try(Dummy(s.toDouble.toInt)).fold(
      _     => Left("unparsable dummy"),
      value => Right(value)
    )
    implicit def dummyShow: Show[Dummy] = Show(_.value.toString)
    case class Dummy(value: Int)
  }

  "Parse#parse" must {
    "use the defined implicit" in new TestSetup {
      check(forAll(Gen.choose(minValue, maxValue)) { i =>
        Prop(Parser.parse(s"$i") == Right(Dummy(i)))
      })
    }
    "invalid parsing" in new TestSetup {
      Parser.parse("bloerk") mustBe Left("unparsable dummy")
    }
  }

  "Parser#uuidParse" must {
    "return a valid UUID" in {
      Parser.uuidParse.parse("a17e27cb-fd74-416f-9be5-f5a34a08df34") mustBe
        Right(UUID.fromString("a17e27cb-fd74-416f-9be5-f5a34a08df34"))
    }
    "unparsable uuid" in {
      Parser.uuidParse.parse("bloerk") mustBe Left("parse.expected.uuid")
    }
  }

  "Parser#intParse" must {
    Seq(
      "0"           -> Right(0),
      "-1"          -> Right(-1),
      "1"           -> Right(1),
      "10345"       -> Right(10345),
      "-243510345"  -> Right(-243510345),
      " 434"        -> Right(434),
      " 43554 "     -> Right(43554),
      "2147483647"  -> Right(2147483647),
      "-2147483648" -> Right(-2147483648),
      "2147483648"  -> Left("parse.expected.int"),
      "-2147483649" -> Left("parse.expected.int"),
      ""            -> Left("parse.expected.int"),
      "-243df515"   -> Left("parse.expected.int"),
      "12.2"        -> Left("parse.expected.int"),
      "-12.0"       -> Left("parse.expected.int"),
      "123what"     -> Left("parse.expected.int"),
      "notanumber"  -> Left("parse.expected.int"),
    ).foreach { case (intStr, expected) => s"parse '$intStr' and return $expected" in {
      Parser.intParse.parse(intStr) mustBe expected
    }}
  }

  "Parser#longParse" must {
    Seq(
      "0"           -> Right(0),
      "-1"          -> Right(-1),
      "1"           -> Right(1),
      "10345"       -> Right(10345),
      "-243510345"  -> Right(-243510345),
      " 434"        -> Right(434),
      " 43554 "     -> Right(43554),
      "9223372036854775807"  -> Right(9223372036854775807L),
      "-9223372036854775808" -> Right(-9223372036854775808L),
      "9223372036854775808"  -> Left("parse.expected.long"),
      "-9223372036854775809" -> Left("parse.expected.long"),
      ""            -> Left("parse.expected.long"),
      "-243df515"   -> Left("parse.expected.long"),
      "12.2"        -> Left("parse.expected.long"),
      "-12.0"       -> Left("parse.expected.long"),
      "123what"     -> Left("parse.expected.long"),
      "notanumber"  -> Left("parse.expected.long"),
    ).foreach { case (longStr, expected) => s"parse '$longStr' and return $expected" in {
      Parser.longParse.parse(longStr) mustBe expected
    }}
  }

  "Parse#floatParse" must {
    List(
      "0.0"                   -> Right(0),
      "-1"                    -> Right(-1),
      "1"                     -> Right(1),
      "10345"                 -> Right(10345),
      "-243510345"            -> Right(-243510345),
      " 434"                  -> Right(434.0),
      " 43554 "               -> Right(43554.0),
      "9.223372036854775807"  -> Right(9.223372036854775807F),
      "-9.223372036854775808" -> Right(-9.223372036854775808F),
      "95720934579245872945876092458637049568739054687390586730945867390856" -> Left("parse.expected.float"),
      "9.223372"              -> Right(9.223372F),
      " 9.223372036854775807" -> Right(9.223372036854775807F),
      " 9.2233"               -> Right(9.2233F),
      "-9.2233 "              -> Right(-9.2233F),
      "9.2233-"               -> Left("parse.expected.float"),
      ""                      -> Left("parse.expected.float"),
      "-243df515"             -> Left("parse.expected.float"),
      "12,2"                  -> Left("parse.expected.float"),
      "-12,0"                 -> Left("parse.expected.float"),
      "123what"               -> Left("parse.expected.float"),
      "notanumber"            -> Left("parse.expected.float"),
    ).foreach { case (floatStr, expected) => s"parse '$floatStr' and return $expected" in {
      Parser.floatParse.parse(floatStr) mustBe expected
    }}
  }

  "Parser#stringParse" must {
    "pass any string" in {
      check(forAll(Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)) { s =>
        Prop(Parser.stringParse.parse(s) == Right(s))
      })
    }
  }

  "Parser#enumParse" must {
    object MyCoolEnum extends Enumeration {
      type MyCoolEnum = Value
      val ENUM_X: MyCoolEnum = Value("der xte")
      val ENUM_Y: MyCoolEnum = Value("mr. y")
    }

    "work very good good good" in {
      Parser.enumNameParse(MyCoolEnum).parse("der xte") mustBe Right(MyCoolEnum.ENUM_X)
      Parser.enumNameParse(MyCoolEnum).parse("mr. y") mustBe Right(MyCoolEnum.ENUM_Y)
      Parser.enumNameParse(MyCoolEnum).parse("dork") mustBe Left("parse.expected.enum")
    }
  }

  "Parser#covariant functor" must {
    implicit val parser: Parser[String] = Parser.stringParse
    val gen = Gen.oneOf((1 to 100).map(_.toString))
    def f: String => Float  = _.toFloat * 2
    def g: Float  => String = o => (o - 5).toString

    "fulfill identity covariant functor law:  id = id" in {
      check(forAll(gen) { s =>
        Prop(parser.map(identity[String]).parse(s) === parser.parse(s))
      })
    }
    "fulfill composing covariant functor law: v.map(f).map(g) = map(f(g))" in {
      check(forAll(gen) { s =>
        Prop(parser.map(f).map(g).parse(s) === parser.map(f.map(g)).parse(s))
      })
    }
  }

  "Parser#zonedDateTimeParser" must {
    val formatter = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of("UTC"))

    "return a valid ZonedDateTime" in {
      Parser.zonedDateTimeParser(formatter).parse("2020-01-14T11:21:10.591778+01:00[Europe/Berlin]") mustBe
        Right(ZonedDateTime.parse("2020-01-14T11:21:10.591778+01:00[Europe/Berlin]"))
    }
    "return an error when invalid ZonedDateTime is given" in {
      Parser.zonedDateTimeParser(formatter).parse("derp") mustBe
        Left("parse.expected.zoneddatetime")
    }
  }

  "Parser#localDateTimeParser" must {
    val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")

    "return a valid LocalDateTime" in {
      Parser.localDateTimeParser(formatter).parse("14.01.2020 11:21:10") mustBe
        Right(LocalDateTime.parse("2020-01-14T11:21:10"))
    }
    "return an error when invalid LocalDateTime is given" in {
      Parser.localDateTimeParser(formatter).parse("derp") mustBe
        Left("parse.expected.localdatetime")
    }
  }

}
