package org.ronruby.lib.csv

import cats.implicits._
import org.ronruby.lib.csv.ReadResult._
import org.scalatestplus.play.PlaySpec

class ColumnBuilderSpec extends PlaySpec {

  "ColumnBuilder#column" must {
    "return an instance of ColumnBuilder" in {
      ColumnBuilder.column(0) mustBe a[ColumnBuilder]
    }
  }

  "ColumnBuilder#as(read)" must {
    "return a ReadSuccess of Dummy" in {
      case class Dummy(id: String)
      implicit val dummyReads: Reads[Dummy] = Reads.stringReads.read(_).map(Dummy)
      val columnReads = ColumnBuilder.column(0).as[Dummy]
      columnReads.read(List("123").toIndexedSeq) mustBe ReadSuccess(Dummy("123"))
    }
    "return a ReadFailure" in {
      case class Dummy(id: String)
      implicit val dummyReads: Reads[Dummy] = Reads.stringReads.read(_).map(Dummy)
      val columnReads = ColumnBuilder.column(1).as[Dummy]
      columnReads.read(List("123").toIndexedSeq) mustBe a[ReadFailure]
    }
  }

  "ColumnBuilder#asOpt" must {
    "return a ReadSuccess of an optional Dummy" in {
      case class Dummy(id: String)
      implicit val dummyReads: Reads[Dummy] = Reads.stringReads.read(_).map(Dummy)
      val columnReads = ColumnBuilder.column(0).asOpt[Dummy]
      columnReads.read(List("123").toIndexedSeq) mustBe ReadSuccess(Dummy("123").some)
    }
    "return a ReadSuccess but None value of Dummy" in {
      case class Dummy(id: String)
      implicit val dummyReads: Reads[Dummy] = Reads.stringReads.read(_).map(Dummy)
      val columnReads = ColumnBuilder.column(1).asOpt[Dummy]
      columnReads.read(List("123").toIndexedSeq) mustBe ReadSuccess(none[Dummy])
    }
  }

}
