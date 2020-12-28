package org.ronruby.lib.csv

import cats.implicits._
import org.ronruby.lib.csv.ReadResult.{ReadFailure, ReadSuccess}
import org.scalatestplus.play.PlaySpec

class ColumnBuilderSpec extends PlaySpec {

  "ColumnBuilder#column" must {
    "return an instance of ColumnBuilder" in {
      ColumnBuilder.column("id") mustBe a[ColumnBuilder]
    }
  }

  "ColumnBuilder#as" must {
    "return a ReadSuccess of Dummy" in {
      case class Dummy(id: String)
      implicit val dummyReads: Reads[Dummy] = Reads.stringReads.read(_).map(Dummy)
      val columnReads = ColumnBuilder.column("id").as[Dummy]
      columnReads.isHeaderValid("id" :: Nil) mustBe true
      columnReads.read(Seq(Column("id", "123"))) mustBe ReadSuccess(Dummy("123"))
    }
    "return a ReadFailure" in {
      case class Dummy(id: String)
      implicit val dummyReads: Reads[Dummy] = Reads.stringReads.read(_).map(Dummy)
      val columnReads = ColumnBuilder.column("id").as[Dummy]
      columnReads.isHeaderValid("derp" :: Nil) mustBe false
      columnReads.read(Seq(Column("derp", "123"))) mustBe a[ReadFailure]
    }
  }

  "ColumnBuilder#asOpt" must {
    "return a ReadSuccess of an optional Dummy" in {
      case class Dummy(id: String)
      implicit val dummyReads: Reads[Dummy] = Reads.stringReads.read(_).map(Dummy)
      val columnReads = ColumnBuilder.column("id").asOpt[Dummy]
      columnReads.isHeaderValid("id" :: Nil) mustBe true
      columnReads.read(Seq(Column("id", "123"))) mustBe ReadSuccess(Dummy("123").some)
    }
    "return a ReadSuccess but None value of Dummy" in {
      case class Dummy(id: String)
      implicit val dummyReads: Reads[Dummy] = Reads.stringReads.read(_).map(Dummy)
      val columnReads = ColumnBuilder.column("id").asOpt[Dummy]
      columnReads.isHeaderValid("derp" :: Nil) mustBe true
      columnReads.read(Seq(Column("derp", "123"))) mustBe ReadSuccess(none[Dummy])
    }
  }

}
