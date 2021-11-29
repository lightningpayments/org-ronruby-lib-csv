package de.lightningpayments.lib.csvstreams

import cats.implicits._
import de.lightningpayments.lib.csvstreams.ColumnReads._
import de.lightningpayments.lib.csvstreams.ReadResult.{ReadFailure, ReadSuccess}
import org.apache.spark.sql.Row

import scala.util.Try

final case class ColumnBuilder(index: Int) extends AnyVal

object ColumnBuilder {

  implicit class RichColumnBuilder(val columnBuilder: ColumnBuilder) extends Serializable {
    def as[A](implicit r: Reads[A]): ColumnReads[A] = (row: Row) =>
      Try(row.getString(columnBuilder.index))
        .map(r.read)
        .getOrElse(ReadFailure(s"Column ${columnBuilder.index} does not exist."))

    def asOpt[A](implicit r: Reads[A]): ColumnReads[Option[A]] = (row: Row) =>
      Try(row.getString(columnBuilder.index))
        .filter(_.nonEmpty)
        .map(r.read)
        .fold(
          fa = _ => ReadSuccess(None),
          fb = _.map(Some(_))
        )
  }

  def column(index: Int): ColumnBuilder = ColumnBuilder(index)

}
