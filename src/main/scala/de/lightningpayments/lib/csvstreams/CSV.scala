package de.lightningpayments.lib.csvstreams

import de.lightningpayments.lib.csvstreams.ColumnReads._
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}

object CSV {

  private final val DELIMITER_KEY = "delimiter"
  private final val HEADER_KEY = "header"

  def parse[T](
    path: String,
    delimiter: String = ";",
    header: Boolean = false)(
    implicit sparkSession: SparkSession,
    encoder: Encoder[T],
    cr: ColumnReads[T]
  ): Dataset[T] =
    sparkSession
      .read
      .option(DELIMITER_KEY, delimiter)
      .option(HEADER_KEY, header)
      .csv(path)
      .flatMap[T]((row: Row) => cr.read(row) match {
        case ReadResult.ReadSuccess(t) => t :: Nil
        case ReadResult.ReadFailure(_) => Nil
      })

}
