package de.lightningpayments.lib.csvstreams

import cats.implicits._
import de.lightningpayments.lib.csvstreams.ReadResult.{ReadFailure, ReadSuccess}

import scala.util.Try

final case class ColumnBuilder(index: Int) extends AnyVal

object ColumnBuilder {

  implicit class RichColumnBuilder(columnBuilder: ColumnBuilder) extends Serializable {
    def as[T](implicit r: Reads[T]): ColumnReads[T] = row =>
      Try(row.getString(columnBuilder.index))
        .map(r.read)
        .getOrElse(ReadFailure(s"Column ${columnBuilder.index} does not exist."))

    def asOpt[T](implicit r: Reads[T]): ColumnReads[Option[T]] = row =>
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
