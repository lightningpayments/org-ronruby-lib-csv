package de.lightningpayments.lib.csvstreams

import de.lightningpayments.lib.csvstreams.ColumnBuilder._
import de.lightningpayments.lib.csvstreams.ColumnReads._
import de.lightningpayments.lib.csvstreams.ReadResult._
import de.lightningpayments.lib.csvstreams.Reads._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import play.api.libs.functional.syntax._
import zio.{Task, ZIO}

import java.nio.file.Paths
import scala.io.Source

class CsvParserSpec extends TestSpec with SparkTestSupport {

  case class Person(name: String, age: Int, city: Option[String])
  object Person {
    implicit val personEncoder: Encoder[Person] = Encoders.product
  }

  case class Maximum(
      p1: Int, p2: Int, p3: Int, p4: Int, p5: Int, p6: Int, p7: Int, p8: Int, p9: Int, p10: Int,
      p11: Int, p12: Int, p13: Int, p14: Int, p15: Int, p16: Int, p17: Int, p18: Int, p19: Int,
      p20: Int, p21: Int, p22: Int
  )
  object Maximum {
    implicit val maximumEncoder: Encoder[Maximum] = Encoders.product
  }

  "CsvParser#parseStream" must {
    "parse 22 params case class" in withSparkSession { implicit spark =>
      import Maximum._

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

      val path = Paths.get("maximum.csv").toString
      val io = ZIO(CsvParser.parse(path = path, delimiter = ",", header = true).collect().toList)
      whenReady(io)(_ mustBe Right(Seq(
        Maximum(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
      )))
    }
  }

  "CsvParser#parseStream(testReads)" must {

    implicit val personReads: ColumnReads[Person] = (
      column(0).as[String] ~
      column(1).as[Int] ~
      column(2).asOpt[String]
    ) (Person.apply _)

    "empty string" in withSparkSession { implicit spark =>
      import Person._

      val path = Paths.get("empty.csv").toString
      val io = ZIO(CsvParser.parse(path = path, delimiter = ",", header = true).collect().toList)
      whenReady(io)(_ mustBe Right(Nil))
    }
    // "multiple lines" in {
    //   val csv =
    //     """name,age,city
    //       |john,33,london
    //       |smith,15,birmingham""".stripMargin
    //   val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
    //   whenReady(programParseStream(source, CsvParsing.Comma))(_ mustBe Right(Seq(
    //     Person("john", 33, Some("london")),
    //     Person("smith", 15, Some("birmingham"))
    //   )))
    // }
    // "optional column does not exist" in {
    //   val csv =
    //     """name,age
    //       |john,33""".stripMargin
    //   val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
    //   whenReady(programParseStream(source, CsvParsing.Comma))(_ mustBe Right(Seq(
    //     Person("john", 33, None)
    //   )))
    // }
    // "optional column is empty" in {
    //   val csv =
    //     """name,age,city
    //       |john,33,""".stripMargin
    //   val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
    //   whenReady(programParseStream(source, CsvParsing.Comma))(_ mustBe Right(Seq(
    //     Person("john", 33, None)
    //   )))
    // }
    // "ignore unused columns" in {
    //   val csv =
    //     """derp
    //       |name,age,city
    //       |john,,london
    //       |john,33,london
    //     """.stripMargin
    //   val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
    //   whenReady(programParseStream(source, CsvParsing.Comma)) {
    //     case Left(err) => err mustBe a[ReadFailure]
    //     case Right(_) => fail("programParseStream: ignore unused columns")
    //   }
    // }
    // "ignore specific quotes type in lines" in {
    //   val csv =
    //     """name,age,city
    //       |john,33,"london
    //       |smith,15,"birmingham""".stripMargin
    //   val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
    //   whenReady(
    //     Task.fromFuture(_ =>
    //       CsvParser
    //         .parseStream(csv = source, separator = CsvParsing.Comma, 1, Some('\''))
    //         .runWith(Sink.seq)
    //     )
    //   )(_ mustBe Right(Vector(Person("john", 33, Some(""""london""")), Person("smith", 15, Some(""""birmingham""")))))
    // }
    // "drop leading lines to read CSV successfully" in {
    //   val csv =
    //     """derp
    //       |name,age,city
    //       |john,,london
    //       |john,33,london
    //     """.stripMargin
    //   val source: Source[ByteString, NotUsed] = Source.single(ByteString(csv))
    //   whenReady(programParseStream(source, CsvParsing.Comma, 2)) {
    //     case Left(err) => err mustBe a[ReadFailure]
    //     case Right(_) => fail("programParseStream: ignore unused columns")
    //   }
    // }
  }

}
