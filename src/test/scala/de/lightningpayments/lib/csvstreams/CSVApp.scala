package de.lightningpayments.lib.csvstreams

import de.lightningpayments.lib.csvstreams.ColumnBuilder._
import de.lightningpayments.lib.csvstreams.ColumnReads._
import de.lightningpayments.lib.csvstreams.Reads._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import play.api.libs.functional.syntax._
import zio.{ExitCode, Task, URIO}

import java.nio.file.Paths

object CSVApp extends zio.App {
  self =>

  private implicit val spark: SparkSession =
    SparkSession.builder().appName("CsvTestApp").master("local[*]").getOrCreate()

  private val path = Paths.get(self.getClass.getResource("/csv/maximum.csv").getPath)

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

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    Task(CSV.parse[Maximum](path = path.normalize().toString, delimiter = ",", header = true))
      .map(_.show())
      .exitCode
  }
}
