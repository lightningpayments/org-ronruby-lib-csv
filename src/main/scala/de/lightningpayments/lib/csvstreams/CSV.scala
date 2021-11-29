package de.lightningpayments.lib.csvstreams

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

import scala.reflect.ClassTag

object CSV {

  private final val DELIMITER_KEY = "delimiter"
  private final val HEADER_KEY = "header"

  def parse[T: ClassTag](
    path: String,
    delimiter: String = ";",
    header: Boolean = false)(
    implicit sparkSession: SparkSession,
    encoder: Encoder[T],
    cr: ColumnReads[T]
  ): RDD[T] =
    sparkSession
      .read
      .option(DELIMITER_KEY, delimiter)
      .option(HEADER_KEY, header)
      .csv(path)
      .rdd
      .map(cr.read(_) match {
        case ReadResult.ReadSuccess(t) => sparkSession.createDataset(Seq(t)).rdd
        case ReadResult.ReadFailure(_) => sparkSession.emptyDataset[T].rdd
      })
      .flatMap[T](_.toLocalIterator)

}
