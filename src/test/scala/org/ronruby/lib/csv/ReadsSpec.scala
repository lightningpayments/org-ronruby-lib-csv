package org.ronruby.lib.csv

import org.ronruby.lib.csv.ReadResult._
import org.ronruby.lib.csv.Reads.{csvReads, _}
import org.scalatestplus.play.PlaySpec

import scala.util.Try

class ReadsSpec extends PlaySpec {

  "default Reads" must {
    "read String" in {
      read[String]("string") mustBe ReadSuccess("string")
    }
    "read Int" in {
      read[Int]("1") mustBe ReadSuccess(1)
      read[Int]("1.0") mustBe a[ReadFailure]
      read[Int]("not an Int") mustBe a[ReadFailure]
    }
    "read Long" in {
      read[Long]("1") mustBe ReadSuccess(1)
      read[Long]("1.0") mustBe a[ReadFailure]
      read[Long]("not an Int") mustBe a[ReadFailure]
    }
    "read Float" in {
      read[Float]("1") mustBe ReadSuccess(1f)
      read[Float]("1.0") mustBe ReadSuccess(1f)
      read[Float]("not a Float") mustBe a[ReadFailure]
    }
    "read Double" in {
      read[Double]("1") mustBe ReadSuccess(1d)
      read[Double]("1.0") mustBe ReadSuccess(1d)
      read[Double]("not a Double") mustBe a[ReadFailure]
    }
    "read Boolean" in {
      read[Boolean]("true") mustBe ReadSuccess(true)
      read[Boolean]("True") mustBe ReadSuccess(true)
      read[Boolean]("TrUe") mustBe ReadSuccess(true)
      read[Boolean]("TRUE") mustBe ReadSuccess(true)
      read[Boolean]("false") mustBe ReadSuccess(false)
      read[Boolean]("False") mustBe ReadSuccess(false)
      read[Boolean]("FalSe") mustBe ReadSuccess(false)
      read[Boolean]("FALSE") mustBe ReadSuccess(false)
      read[Boolean]("not a Boolean") mustBe a[ReadFailure]
    }
  }

  "Parse#csvReads" must {
    "return - expected.string error by int value" in {
      implicit val parse: Parser[Int] = x => Try(x.toDouble.toInt).fold(_ => Left("derp"), Right(_))
      read[Int]("123")(csvReads) mustBe ReadSuccess(123)
    }
    "return - custom error when condition is false" in {
      implicit val parse: Parser[Int] = x => Try(x.toInt).fold(_ => Left("custom error"), Right(_))
      read[Int]("123A")(csvReads) mustBe ReadFailure("custom error")
    }
  }

  def read[T](value: String)(implicit r: Reads[T]): ReadResult[T] = r.read(value)

}
