package org.ronruby.lib.csv

import cats.implicits._
import org.ronruby.lib.csv.ReadResult.{ReadFailure, ReadSuccess}

import scala.util.Try

final case class ColumnBuilder(index: Int) extends AnyVal

object ColumnBuilder {

  implicit class RichColumnBuilder(columnBuilder: ColumnBuilder) {
    def as[T](implicit r: Reads[T]): ColumnReads[T] = (line: IndexedSeq[String]) =>
      Try(line(columnBuilder.index))
        .map(r.read)
        .getOrElse(ReadFailure(s"Column ${columnBuilder.index} does not exist."))

    def asOpt[T](implicit r: Reads[T]): ColumnReads[Option[T]] = (line: IndexedSeq[String]) =>{
      Try(line(columnBuilder.index))
        .filter(_.nonEmpty)
        .map(r.read)
        .fold(
          fa = _ => ReadSuccess(None),
          fb = _.map(Some(_))
        )
    }
  }

  def column(index: Int): ColumnBuilder = ColumnBuilder(index)
}
