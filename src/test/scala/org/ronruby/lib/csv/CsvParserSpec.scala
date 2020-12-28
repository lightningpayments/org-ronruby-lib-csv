package org.ronruby.lib.csv

import org.ronruby.lib.csv.ColumnBuilder._
import org.ronruby.lib.csv.ColumnReads._
import org.ronruby.lib.csv.CsvParser._
import org.ronruby.lib.csv.Reads._
import org.ronruby.lib.csv.errors.FailureThrowable
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.play.PlaySpec
import play.api.libs.functional.syntax._
import zio.Task

class CsvParserSpec extends PlaySpec with ScalaFutures {

  case class Person(name: String, age: Int, city: Option[String])

  "CsvParser" must {
    "parse 22 params case class" in {
      case class Maximum(
        p1: Int, p2: Int, p3: Int, p4: Int, p5: Int, p6: Int, p7: Int, p8: Int, p9: Int, p10: Int,
        p11: Int, p12: Int, p13: Int, p14: Int, p15: Int, p16: Int, p17: Int, p18: Int, p19: Int,
        p20: Int, p21: Int, p22: Int
      )

      val csv =
        """
          |p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22
          |1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
        """.stripMargin

      implicit val columnReads: ColumnReads[Maximum] = (
        column("p1").as[Int] ~
          column("p2").as[Int] ~
          column("p3").as[Int] ~
          column("p4").as[Int] ~
          column("p5").as[Int] ~
          column("p6").as[Int] ~
          column("p7").as[Int] ~
          column("p8").as[Int] ~
          column("p9").as[Int] ~
          column("p10").as[Int] ~
          column("p11").as[Int] ~
          column("p12").as[Int] ~
          column("p13").as[Int] ~
          column("p14").as[Int] ~
          column("p15").as[Int] ~
          column("p16").as[Int] ~
          column("p17").as[Int] ~
          column("p18").as[Int] ~
          column("p19").as[Int] ~
          column("p20").as[Int] ~
          column("p21").as[Int] ~
          column("p22").as[Int]
        ) (Maximum.apply _)

      whenReady(parseWithSeparator[Maximum](csv, ','))(_ mustBe Right(Seq(
        Maximum(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
      )))
    }
  }


  "CsvPars -- testReads" must {
    implicit val personReads: ColumnReads[Person] = (
      column("name").as[String] ~
        column("age").as[Int] ~
        column("city").asOpt[String]
      ) (Person)

    "empty string" in {
      val csv = ""
      whenReady(parseWithSeparator(csv, ','))(_ mustBe Right(Seq.empty[Person]))
      whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(false))
    }

    "multiple lines" in {
      val csv =
        """
          |name,age,city
          |john,33,london
          |smith,15,birmingham
                  """.stripMargin

      whenReady(parseWithSeparator(csv, ','))(_ mustBe Right(Seq(
        Person("john", 33, Some("london")),
        Person("smith", 15, Some("birmingham"))
      )))
      whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(true))
    }

    "inversed columns" in {
      val csv =
        """
          |city,age,name
          |london,33,john
                  """.stripMargin

      whenReady(parseWithSeparator(csv, ','))(_ mustBe Right(Seq(
        Person("john", 33, Some("london"))
      )))
      whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(true))
    }

    "ignore empty lines" in {
      val csv =
        """
          |name,age,city
          |
          |john,33,london
          |
                  """.stripMargin

      whenReady(parseWithSeparator(csv, ','))(_ mustBe Right(Seq(
        Person("john", 33, Some("london"))
      )))
      whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(true))
    }

    "ignore unused columns" in {
      val csv =
        """
          |name,x,age,city,y
          |john,x,33,london,y
                  """.stripMargin

      whenReady(parseWithSeparator(csv, ','))(_ mustBe Right(Seq(
        Person("john", 33, Some("london"))
      )))
      whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(true))
    }

    "optional column does not exist" in {
      val csv =
        """
          |name,age
          |john,33
                  """.stripMargin

      whenReady(parseWithSeparator(csv, ','))(_ mustBe Right(Seq(
        Person("john", 33, None)
      )))
      whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(true))
    }

    "optional column is empty" in {
      val csv =
        """
          |name,age,city
          |john,33,
                  """.stripMargin

      whenReady(parseWithSeparator(csv, ','))(_ mustBe Right(Seq(
        Person("john", 33, None)
      )))
      whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(true))
    }

    "';' as a separator" in {
      val csv =
        """
          |name;age;city
          |john;33;london
                  """.stripMargin

      whenReady(parseWithSeparator(csv, ';'))(_ mustBe Right(Seq(
        Person("john", 33, Some("london"))
      )))
      whenReady(isHeaderValidWithSeparator(csv, ';'))(_ mustBe Right(true))
    }
  }

  "CsvParser#failure parsing" must {
    implicit val personReads: ColumnReads[Person] = (
      column("name").as[String] ~
        column("age").as[Int] ~
        column("city").asOpt[String]
      ) (Person)

    "required column does not exist" in {
      implicit val personReads: ColumnReads[Person] = (
        column("name").as[String] ~
          column("age").as[Int] ~
          column("city").asOpt[String]
        ) (Person)
      val csv =
        """
          |name,city
          |john,london
        """.stripMargin

      whenReady(parseWithSeparator(csv, ',')) {
        case Left(failure: FailureThrowable) =>
          failure.lineNum mustBe 0
          failure.line mustBe "john,london"
          failure.message mustBe "Column 'age' does not exist."
          whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(false))
        case Left(_) => fail("Left: required column does not exist")
        case Right(_) => fail("Right: required column does not exist")
      }
    }

    "cannot convert column to required type" in {
      val csv = """
                  |name,age,city
                  |john,x,london
            """.stripMargin
      whenReady(parseWithSeparator(csv, ',')) {
        case Left(failure: FailureThrowable) =>
          failure.lineNum mustBe 0
          failure.line mustBe "john,x,london"
          failure.message mustBe "Cannot convert 'x' to 'int' for column 'age'"
          whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(true))
        case Left(_) => fail("Left: cannot convert column to required type")
        case Right(_) => fail("Right: cannot convert column to required type")
      }
    }
    "more than one required column is missing" in {
      val csv = """
                  |city
                  |london
            """.stripMargin
      whenReady(parseWithSeparator(csv, ',')) {
        case Left(failure: FailureThrowable) =>
          failure.lineNum mustBe 0
          failure.line mustBe "london"
          failure.message mustBe "Column 'age' does not exist., Column 'name' does not exist."
          whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(false))
        case Left(_) => fail("Left: more than one required column is missing")
        case Right(_) => fail("Right: more than one required column is missing")
      }
    }
    "failure on second line" in {
      val csv = """
                  |name,age,city
                  |john,15,london
                  |smith
            """.stripMargin
      whenReady(parseWithSeparator(csv, ',')) {
        case Left(failure: FailureThrowable) =>
          failure.lineNum mustBe 1
          failure.line mustBe "smith"
          failure.message mustBe "Column 'age' does not exist."
          whenReady(isHeaderValidWithSeparator(csv, ','))(_ mustBe Right(true))
        case Left(_) => fail("Left: failure on second line")
        case Right(_) => fail("Right: failure on second line")
      }
    }
  }

  "CsvParser#write" must {
    "return a csv of persons" in {
      val persons = List(
        Person("peter", 31, Some("ScalaCity")),
        Person("peter", 31, Some("ScalaCity")),
        Person("peter", 31, Some("ScalaCity")),
        Person("peter", 31, Some("ScalaCity")),
        Person("peter", 31, Some("ScalaCity")),
        Person("peter", 31, Some("ScalaCity")),
        Person("peter", 31, Some("ScalaCity")),
        Person("peter", 31, Some("ScalaCity")),
        Person("peter", 31, Some("ScalaCity")),
        Person("peter", 31, Some("ScalaCity"))
      )
      val personsCsv =
        """
          |name|age|city
          |host|31|ScalaCity
          |host|31|ScalaCity
          |host|31|ScalaCity
          |host|31|ScalaCity
          |host|31|ScalaCity
          |host|31|ScalaCity
          |host|31|ScalaCity
          |host|31|ScalaCity
          |host|31|ScalaCity
          |host|31|ScalaCity
          |""".stripMargin
      val header = "id" :: "produktId" :: "name" :: "beschreibung" :: "preis" :: "bestand" :: Nil
      CsvParser.write[Person](header, '|', persons) _ mustBe Right(personsCsv.trim)
    }
  }

  private def whenReady[T, U](task: => Task[T])(f: Either[Throwable, T] => U): U =
    whenReady(defaultRuntime.unsafeRunToFuture(task.either))(f)

  private val defaultRuntime: zio.Runtime[zio.ZEnv] = zio.Runtime.default
}
