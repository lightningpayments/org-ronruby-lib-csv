package org.ronruby.lib.csv

import org.ronruby.lib.csv.ReadResult._

import scala.util.Try

trait Reads[T] {
def read(value: String): ReadResult[T]
}

object Reads {

  implicit val stringReads: Reads[String] = (value: String) => ReadSuccess(value)

  implicit val intReads: Reads[Int] = tryRead(_.toInt)

  implicit val longReads: Reads[Long] = tryRead(_.toLong)

  implicit val floatReads: Reads[Float] = tryRead(_.toFloat)

  implicit val doubleReads: Reads[Double] = tryRead(_.toDouble)

  implicit val booleanReads: Reads[Boolean] = tryRead(_.toBoolean)

  implicit def csvReads[T: Parse]: Reads[T] = {
    value => implicitly[Parse[T]].parse(value) match {
      case Right(t) => ReadSuccess(t)
      case Left(err) => ReadFailure(err)
    }
  }

  private def tryRead[T](f: String => T): Reads[T] = (value: String) =>
    Try(ReadSuccess(f(value))).toOption match {
      case None => ReadFailure("convert.error.csv")
      case Some(success) => success
    }

}
