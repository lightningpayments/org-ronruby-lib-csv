package org.ronruby.lib.csv

import cats.implicits._
import ReadResult.{ReadFailure, ReadSuccess}

final case class ColumnBuilder(name: String) extends AnyVal

object ColumnBuilder {

  implicit class RichColumnBuilder(columnBuilder: ColumnBuilder) {
    def as[T](implicit r: Reads[T]): ColumnReads[T] = new ColumnReads[T] {
      override def isHeaderValid(names: Seq[String]): Boolean = names.contains(columnBuilder.name)
      override def read(line: Seq[Column]): ReadResult[T] = {
        line.find(_.name == columnBuilder.name)
          .map(r.read)
          .getOrElse(ReadFailure(s"Column '${columnBuilder.name}' does not exist."))
      }
    }

    def asOpt[T](implicit r: Reads[T]): ColumnReads[Option[T]] = new ColumnReads[Option[T]] {
      override def isHeaderValid(names: Seq[String]): Boolean = true
      override def read(line: Seq[Column]): ReadResult[Option[T]] = {
        line.find(column => column.name == columnBuilder.name && column.value.nonEmpty)
          .map(r.read(_).map(Some(_)))
          .getOrElse(ReadSuccess(None))
      }
    }
  }

  def column(name: String): ColumnBuilder = ColumnBuilder(name)

  def from(name: String): ColumnBuilder = column(name)

}
