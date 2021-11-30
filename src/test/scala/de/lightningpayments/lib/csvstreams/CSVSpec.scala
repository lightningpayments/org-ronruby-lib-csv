package de.lightningpayments.lib.csvstreams

import de.lightningpayments.lib.csvstreams.ColumnBuilder._
import de.lightningpayments.lib.csvstreams.ColumnReads._
import de.lightningpayments.lib.csvstreams.Reads._
import org.apache.spark.sql.{Encoder, Encoders}
import play.api.libs.functional.syntax._

import java.nio.file.Paths

class CSVSpec extends SparkTestSpec { self =>

  private case class Maximum(
      p1: Int, p2: Int, p3: Int, p4: Int, p5: Int, p6: Int, p7: Int, p8: Int, p9: Int, p10: Int,
      p11: Int, p12: Int, p13: Int, p14: Int, p15: Int, p16: Int, p17: Int, p18: Int, p19: Int,
      p20: Int, p21: Int, p22: Int
  )
  private implicit val maximumEncoder: Encoder[Maximum] = Encoders.product[Maximum]
  private implicit val maximumReads: ColumnReads[Maximum] = (
    column(index = 0).as[Int] ~
    column(index = 1).as[Int] ~
    column(index = 2).as[Int] ~
    column(index = 3).as[Int] ~
    column(index = 4).as[Int] ~
    column(index = 5).as[Int] ~
    column(index = 6).as[Int] ~
    column(index = 7).as[Int] ~
    column(index = 8).as[Int] ~
    column(index = 9).as[Int] ~
    column(index = 10).as[Int] ~
    column(index = 11).as[Int] ~
    column(index = 12).as[Int] ~
    column(index = 13).as[Int] ~
    column(index = 14).as[Int] ~
    column(index = 15).as[Int] ~
    column(index = 16).as[Int] ~
    column(index = 17).as[Int] ~
    column(index = 18).as[Int] ~
    column(index = 19).as[Int] ~
    column(index = 20).as[Int] ~
    column(index = 21).as[Int]
  ) (Maximum.apply _)

  private case class Person(name: String, age: Int, city: Option[String])
  private implicit val personEncoder: Encoder[Person] = Encoders.product[Person]
  private implicit val personReads: ColumnReads[Person] =
    (column(index = 0).as[String] ~ column(index = 1).as[Int] ~ column(index = 2).asOpt[String]) (Person.apply _)

  test("CsvParser#parse") {
    val path = Paths.get(self.getClass.getResource("/csv/maximum.csv").getPath).normalize().toString

    assert(
      CSV.parse[Maximum](path = path, delimiter = ",", header = true).collect().toList == List(
        Maximum(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
      )
    )
  }

  // "CsvParser#parse(testReads)" must {
  //   "empty string" in withSparkSession { implicit spark =>
  //     val p = for {
  //       path <- Task(Paths.get(self.getClass.getResource("/csv/empty.csv").getPath))
  //       ds   <- Task(CSV.parse[Person](path = path.normalize().toString, delimiter = ",", header = true))
  //       r    <- Task(ds.collect().toList)
  //     } yield r
  //
  //     // whenReady(p)(_ mustBe Right(Nil))
  //     whenReady(p) {
  //       case Left(ex) => println(ex.getCause().toString)
  //       case _ => fail()
  //     }
  //   }
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
  // }

}
