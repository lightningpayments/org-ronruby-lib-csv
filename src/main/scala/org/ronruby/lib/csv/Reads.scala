package org.ronruby.lib.csv

import ReadResult._

import scala.reflect.ClassTag
import scala.util.Try

trait Reads[T] {
  def read(column: Column): ReadResult[T]
}

object Reads {

  implicit val stringReads: Reads[String] = (column: Column) => ReadSuccess(column.value)

  implicit val intReads: Reads[Int] = tryRead(_.toInt)

  implicit val longReads: Reads[Long] = tryRead(_.toLong)

  implicit val floatReads: Reads[Float] = tryRead(_.toFloat)

  implicit val doubleReads: Reads[Double] = tryRead(_.toDouble)

  implicit val booleanReads: Reads[Boolean] = tryRead(_.toBoolean)

  private def tryRead[T: ClassTag](f: String => T): Reads[T] = (column: Column) =>
    Try(ReadSuccess(f(column.value))).toOption match {
      case Some(success) => success
      case None => ReadFailure(cannotConvertMsg(column, implicitly[ClassTag[T]].runtimeClass.getSimpleName))
    }

  private def cannotConvertMsg(column: Column, typeName: String): String =
    s"Cannot convert '${column.value}' to '$typeName' for column '${column.name}'"
}
