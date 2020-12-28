package org.ronruby.lib.csv

import org.mockito.scalatest.IdiomaticMockito
import org.ronruby.lib.csv.ReadResult.{ReadFailure, ReadSuccess}
import org.ronruby.lib.csv.Reads._
import org.scalatestplus.play.PlaySpec

class ReadsSpec extends PlaySpec with IdiomaticMockito {

  "default Reads" must {
    "read String" in {
      read[String]("string") mustBe ReadSuccess("string")
    }
    "read Int" in {
      read[Int]("1") mustBe ReadSuccess(1)
      read[Int]("1.0") mustBe a[ReadFailure]
      read[Int]("not an Int") mustBe a[ReadFailure]
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

  def read[T](value: String)(implicit r: Reads[T]): ReadResult[T] = r.read(Column("test", value))

}
