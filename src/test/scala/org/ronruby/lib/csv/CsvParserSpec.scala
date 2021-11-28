package org.ronruby.lib.csv

import akka.NotUsed
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.ronruby.lib.csv.ColumnBuilder._
import org.ronruby.lib.csv.ColumnReads._
import org.ronruby.lib.csv.ReadResult._
import org.ronruby.lib.csv.Reads._
import play.api.libs.functional.syntax._
import zio.Task

class CsvParserSpec extends TestSpec with ActorSpec {

  case class Person(name: String, age: Int, city: Option[String])

  private def programParseStream[T: ColumnReads](
    source: Source[ByteString, _],
    separator: Byte,
    dropLeadingLines: Int = 1
  ): Task[Seq[T]] =
    Task.fromFuture(_ =>
      CsvParser
        .parseStream[T](csv = source, separator = separator, dropLeadingLines)
        .runWith(Sink.seq)
    )

  "CsvParser#parseStream" must {
    "parse 22 params case class" in {
      case class Maximum(
        p1: Int, p2: Int, p3: Int, p4: Int, p5: Int, p6: Int, p7: Int, p8: Int, p9: Int, p10: Int,
        p11: Int, p12: Int, p13: Int, p14: Int, p15: Int, p16: Int, p17: Int, p18: Int, p19: Int,
        p20: Int, p21: Int, p22: Int
      )

      val csv =
        """p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22
          |1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22""".stripMargin

      implicit val columnReads: ColumnReads[Maximum] = (
        column(0).as[Int] ~
        column(1).as[Int] ~
        column(2).as[Int] ~
        column(3).as[Int] ~
        column(4).as[Int] ~
        column(5).as[Int] ~
        column(6).as[Int] ~
        column(7).as[Int] ~
        column(8).as[Int] ~
        column(9).as[Int] ~
        column(10).as[Int] ~
        column(11).as[Int] ~
        column(12).as[Int] ~
        column(13).as[Int] ~
        column(14).as[Int] ~
        column(15).as[Int] ~
        column(16).as[Int] ~
        column(17).as[Int] ~
        column(18).as[Int] ~
        column(19).as[Int] ~
        column(20).as[Int] ~
        column(21).as[Int]
      ) (Maximum.apply _)

      val source = Source.single(ByteString(csv))
      whenReady(programParseStream[Maximum](source, CsvParsing.Comma))(_ mustBe Right(Seq(
        Maximum(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
      )))
    }
  }

  "CsvParser#parseStream(testReads)" must {

    implicit val personReads: ColumnReads[Person] = (
      column(0).as[String] ~
      column(1).as[Int] ~
      column(2).asOpt[String]
    ) (Person)

    "empty string" in {
      val csv = ""
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStream(source, CsvParsing.Comma))(
        _ mustBe Right(Nil)
      )
    }
    "multiple lines" in {
      val csv =
        """name,age,city
          |john,33,london
          |smith,15,birmingham""".stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStream(source, CsvParsing.Comma))(_ mustBe Right(Seq(
        Person("john", 33, Some("london")),
        Person("smith", 15, Some("birmingham"))
      )))
    }
    "optional column does not exist" in {
      val csv =
        """name,age
          |john,33""".stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStream(source, CsvParsing.Comma))(_ mustBe Right(Seq(
        Person("john", 33, None)
      )))
    }
    "optional column is empty" in {
      val csv =
        """name,age,city
          |john,33,""".stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStream(source, CsvParsing.Comma))(_ mustBe Right(Seq(
        Person("john", 33, None)
      )))
    }
    "ignore unused columns" in {
      val csv =
        """derp
          |name,age,city
          |john,,london
          |john,33,london
        """.stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStream(source, CsvParsing.Comma)) {
        case Left(err) => err mustBe a[ReadFailure]
        case Right(_) => fail("programParseStream: ignore unused columns")
      }
    }
    "ignore specific quotes type in lines" in {
      val csv =
        """name,age,city
          |john,33,"london
          |smith,15,"birmingham""".stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(
        Task.fromFuture(_ =>
          CsvParser
            .parseStream(csv = source, separator = CsvParsing.Comma, 1, Some('\''))
            .runWith(Sink.seq)
        )
      )(_ mustBe Right(Vector(Person("john", 33, Some(""""london""")), Person("smith", 15, Some(""""birmingham""")))))
    }
    "drop leading lines to read CSV successfully" in {
      val csv =
        """derp
          |name,age,city
          |john,,london
          |john,33,london
        """.stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStream(source, CsvParsing.Comma, 2)) {
        case Left(err) => err mustBe a[ReadFailure]
        case Right(_) => fail("programParseStream: ignore unused columns")
      }
    }
  }

  private def programParseStreamIgnoringFailure[T: ColumnReads](
    source: Source[ByteString, _],
    separator: Byte
  ): Task[Seq[T]] =
    Task.fromFuture(_ => CsvParser.parseStreamIgnoringFailure[T](
      csv = source,
      separator = separator
    ).runWith(Sink.seq))

  "CsvParser#parseStreamIgnoringFailure" must {
    "parse 22 params case class" in {
      case class Maximum(
        p1: Int, p2: Int, p3: Int, p4: Int, p5: Int, p6: Int, p7: Int, p8: Int, p9: Int, p10: Int,
        p11: Int, p12: Int, p13: Int, p14: Int, p15: Int, p16: Int, p17: Int, p18: Int, p19: Int,
        p20: Int, p21: Int, p22: Int
      )

      val csv =
        """p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22
          |1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22""".stripMargin

      implicit val columnReads: ColumnReads[Maximum] = (
        column(0).as[Int] ~
        column(1).as[Int] ~
        column(2).as[Int] ~
        column(3).as[Int] ~
        column(4).as[Int] ~
        column(5).as[Int] ~
        column(6).as[Int] ~
        column(7).as[Int] ~
        column(8).as[Int] ~
        column(9).as[Int] ~
        column(10).as[Int] ~
        column(11).as[Int] ~
        column(12).as[Int] ~
        column(13).as[Int] ~
        column(14).as[Int] ~
        column(15).as[Int] ~
        column(16).as[Int] ~
        column(17).as[Int] ~
        column(18).as[Int] ~
        column(19).as[Int] ~
        column(20).as[Int] ~
        column(21).as[Int]
      ) (Maximum.apply _)

      val source = Source.single(ByteString(csv))
      whenReady(programParseStreamIgnoringFailure[Maximum](source, CsvParsing.Comma))(_ mustBe Right(Seq(
        Maximum(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
      )))
    }
  }


  "CsvPars#parseStreamIgnoringFailure(testReads)" must {

    implicit val personReads: ColumnReads[Person] = (
      column(0).as[String] ~
      column(1).as[Int] ~
      column(2).asOpt[String]
    ) (Person)

    "empty string" in {
      val csv = ""
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStreamIgnoringFailure(source, CsvParsing.Comma))(
        _ mustBe Right(Nil)
      )
    }
    "multiple lines" in {
      val csv =
        """name,age,city
          |john,33,london
          |smith,15,birmingham""".stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStreamIgnoringFailure(source, CsvParsing.Comma))(_ mustBe Right(Seq(
        Person("john", 33, Some("london")),
        Person("smith", 15, Some("birmingham"))
      )))
    }
    "ignore empty lines" in {
      val csv =
        """
          |name,age,city
          |
          |john,33,london
          |""".stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStreamIgnoringFailure(source, CsvParsing.Comma))(_ mustBe Right(Seq(
        Person("john", 33, Some("london"))
      )))
    }
    "ignore specific quotes type in lines" in {
      implicit val personReads: ColumnReads[Person] = (
        column(0).as[String] ~
          column(1).as[Int] ~
          column(2).asOpt[String]
        ) (Person)
      val csv =
        """derp
          |name,age,city
          |john,33,"london
          |smith,15,"birmingham
        """.stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(Task.fromFuture(_ => CsvParser.parseStreamIgnoringFailure(
        csv = source,
        separator = CsvParsing.Comma,
        ignoreQuoteChar = Some('\'')
      ).runWith(Sink.seq)))(_ mustBe Right(Seq(
        Person("john", 33, Some(""""london""")),
        Person("smith", 15, Some(""""birmingham"""))
      )))
    }
    "ignore unused columns" in {
      val csv =
        """derp
          |name,age,city
          |john,,london
          |john,33,london
        """.stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStreamIgnoringFailure(source, CsvParsing.Comma))(_ mustBe Right(Seq(
        Person("john", 33, Some("london"))
      )))
    }
    "ignore empty columns as empty list" in {
      val csv =
        """derp
          |name,age,city
          |john,,london
          |john,derp,london
        """.stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStreamIgnoringFailure(source, CsvParsing.Comma))(_ mustBe Right(Nil))
    }
    "ignore broken lines" in {
      val csv =
        """derp
          |name,age,city
          |john,
          |john,derp,london
          |john,33,london
        """.stripMargin
      val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
      whenReady(programParseStreamIgnoringFailure(source, CsvParsing.Comma))(_ mustBe Right(Seq(
        Person("john", 33, Some("london"))
      )))
    }
  }

  case class Data(
      shopId: Int,
      articleId: Int,
      stock: String,
      price: String
  )

  object Data {
    implicit val reads: ColumnReads[Data] = (
      column(0).as[Int] ~
      column(1).as[Int] ~
      column(2).as[String] ~
      column(3).as[String]
     )((o1, o2, o3, o4) => Data(o1, o2, o3, o4))
  }

  private val txt =
    """
      |412239;445531;0,00;7,99;;
      |412239;445440;0,00;6,99;;
      |412239;445479;0,000;0,99;;
      |412239;445532;0,00;7,99;;
      |412239;445533;0,00;7,99;;
      |412239;445534;1,00;9,99;;
      |412239;445515;0,000;2,99;;
      |""".stripMargin

  "CsvParser#parseStreamIgnoringFailure" must {
    "parse data as expected" in {
      val source = Source.single(ByteString(txt))
      val foo = CsvParser.parseStreamIgnoringFailure[Data](
        csv = source,
        separator = CsvParsing.SemiColon,
        dropLeadingLines = 0
      )

      whenReady(foo.runWith(Sink.seq))(_.size mustBe 7)
    }
  }

}
