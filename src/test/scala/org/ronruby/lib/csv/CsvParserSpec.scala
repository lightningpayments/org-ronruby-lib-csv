package org.ronruby.lib.csv

import cats.implicits._
import org.ronruby.lib.csv.ColumnBuilder._
import org.ronruby.lib.csv.ColumnReads._
import org.ronruby.lib.csv.CsvParser._
import org.ronruby.lib.csv.Reads._
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.play.PlaySpec
import play.api.libs.functional.FunctionalBuilder
import play.api.libs.functional.syntax._

class CsvParserSpec extends PlaySpec with AnyFunSpec {

  case class Person(name: String, age: Int, city: Option[String])

  "CsvParser" must {
    "manually defined reads" in {
      val personReads: ColumnReads[Person] = (
        column("name").as[String] ~
        column("age").as[Int] ~
        column("city").asOpt[String]
      ) (Person)

      testReads(personReads)
    }
  }

  case class Person(name: String, age: Int, city: Option[String])

  describe("manually defined reads") {
    val personReads: ColumnReads[Person] = (
      column("name").as[String] and
        column("age").as[Int] and
        column("city").asOpt[String]
      )(Person)

    testReads(personReads)
  }

  describe("maximum size case class") {
    it("parse 22 params case class") {
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

      val columns1 =
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
        column("p11").as[Int]


      val columns2 =
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

      implicit val columnReads: ColumnReads[Maximum] = (
        columns1 ~
        columns2
      )(Maximum.apply _)

      parseWithSeparator[Maximum](csv, ',') mustBe Right(Seq(
        Maximum(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
      ))
    }
  }

  def testReads(implicit cr: ColumnReads[Person]): Unit = {
    describe("success parsing") {
      it("empty string") {
        val csv = ""
        parseWithSeparator(csv, ',') mustBe Right(Seq.empty[Person])
        isHeaderValidWithSeparator(csv, ',') mustBe false
      }

      it("multiple lines") {
        val csv = """
                    |name,age,city
                    |john,33,london
                    |smith,15,birmingham
                  """.stripMargin

        parseWithSeparator(csv, ',') mustBe Right(Seq(
          Person("john", 33, Some("london")),
          Person("smith", 15, Some("birmingham"))
        ))
        isHeaderValidWithSeparator(csv, ',') mustBe true
      }

      it("inversed columns") {
        val csv = """
                    |city,age,name
                    |london,33,john
                  """.stripMargin

        parseWithSeparator(csv, ',') mustBe Right(Seq(
          Person("john", 33, Some("london"))
        ))
        isHeaderValidWithSeparator(csv, ',') mustBe true
      }

      it("ignore empty lines") {
        val csv = """
                    |name,age,city
                    |
                    |john,33,london
                    |
                  """.stripMargin

        parseWithSeparator(csv, ',') mustBe Right(Seq(
          Person("john", 33, Some("london"))
        ))
        isHeaderValidWithSeparator(csv, ',') mustBe true
      }

      it("ignore unused columns") {
        val csv = """
                    |name,x,age,city,y
                    |john,x,33,london,y
                  """.stripMargin

        parseWithSeparator(csv, ',') mustBe Right(Seq(
          Person("john", 33, Some("london"))
        ))
        isHeaderValidWithSeparator(csv, ',') mustBe true
      }

      it("optional column does not exist") {
        val csv = """
                    |name,age
                    |john,33
                  """.stripMargin

        parseWithSeparator(csv, ',') mustBe Right(Seq(
          Person("john", 33, None)
        ))
        isHeaderValidWithSeparator(csv, ',') mustBe true
      }

      it("optional column is empty") {
        val csv = """
                    |name,age,city
                    |john,33,
                  """.stripMargin

        parseWithSeparator(csv, ',') mustBe Right(Seq(
          Person("john", 33, None)
        ))
        isHeaderValidWithSeparator(csv, ',') mustBe true
      }

      it("';' as a separator") {
        val csv = """
                    |name;age;city
                    |john;33;london
                  """.stripMargin

        parseWithSeparator(csv, ';') mustBe Right(Seq(
          Person("john", 33, Some("london"))
        ))
        isHeaderValidWithSeparator(csv, ';') mustBe true
      }
    }

    describe("failure parsing") {
      it("required column does not exist") {
        val csv = """
                    |name,city
                    |john,london
                  """.stripMargin

        val result = parse(csv)

        val failure = expectFailure(result)
        failure.lineNum shouldBe 0
        failure.line shouldBe "john,london"
        failure.message should include("age")

        isHeaderValid(csv) shouldBe false
      }

      it("cannot convert column to required type") {
        val csv = """
                    |name,age,city
                    |john,x,london
                  """.stripMargin

        val result = parse(csv)

        val failure = expectFailure(result)
        failure.lineNum shouldBe 0
        failure.line shouldBe "john,x,london"
        failure.message should include("age")

        isHeaderValid(csv) shouldBe true
      }

      it("more than one required column is missing") {
        val csv = """
                    |city
                    |london
                  """.stripMargin

        val result = parse(csv)

        val failure = expectFailure(result)
        failure.lineNum shouldBe 0
        failure.line shouldBe "london"
        failure.message should (include("name") and include("age"))

        isHeaderValid(csv) shouldBe false
      }

      it("failure on second line") {
        val csv = """
                    |name,age,city
                    |john,15,london
                    |smith
                  """.stripMargin

        val result = parse(csv)

        val failure = expectFailure(result)
        failure.lineNum shouldBe 1
        failure.line shouldBe "smith"
        failure.message should include("age")

        isHeaderValid(csv) shouldBe true
      }
    }
  }

  private def expectFailure[A](result: Either[A, _]): A =
    result.left.getOrElse(fail(s"Expected failure, but got $result"))
}
