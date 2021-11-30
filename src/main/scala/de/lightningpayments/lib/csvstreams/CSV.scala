package de.lightningpayments.lib.csvstreams

import de.lightningpayments.lib.csvstreams.ColumnReads._
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}

object CSV {

  private final val DELIMITER_KEY = "delimiter"
  private final val HEADER_KEY = "header"

  def parse[A](
    path: String,
    delimiter: String = ";",
    header: Boolean = false)(
    implicit sparkSession: SparkSession,
    encoder: Encoder[A],
    cr: ColumnReads[A]
  ): Dataset[A] =
    sparkSession.read.option(DELIMITER_KEY, delimiter).option(HEADER_KEY, header).csv(path).flatMap[A](
      (row: Row) =>
        cr.read(row) match {
          case ReadResult.ReadSuccess(a) => List.apply[A](a)
          case ReadResult.ReadFailure(_) => List.empty[A]
        }
    )

}
