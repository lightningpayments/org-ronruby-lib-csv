package org.ronruby.lib.csv

import cats.implicits._
import cats.{Functor, SemigroupK}

import java.util.UUID
import scala.util.Try

trait Parse[T] {
  def parse(s: String): Either[String, T]
  def withError(errorMsg: String): Parse[T] = parse(_).leftMap(_ => errorMsg)
}

object Parse {

  private val intAndLongPattern = "^\\s*(-?\\d+)\\s*$".r

  private val floatPattern = "^\\s*([+-]?([0-9]*[.])?[0-9]+)\\s*$".r

  implicit val functor: Functor[Parse] = new Functor[Parse] {
    override def map[A, B](fa: Parse[A])(f: A => B): Parse[B] = fa.parse(_).map(f(_))
  }

  implicit val semigroupK: SemigroupK[Parse] = new SemigroupK[Parse] {
    override def combineK[A](x: Parse[A], y: Parse[A]): Parse[A] = s => x.parse(s) <+> y.parse(s)
  }

  def parse[T: Parse](s: String): Either[String, T] = implicitly[Parse[T]].parse(s)

  def uuidParse: Parse[UUID] = uuid => Try(UUID.fromString(uuid)).fold(_ => Left("parse.expected.uuid"), Right(_))

  def intParse: Parse[Int] = {
    case intAndLongPattern(s) => Try(s.toInt).toEither.leftMap(_ => "parse.expected.int")
    case _                    => Left("parse.expected.int")
  }

  def longParse: Parse[Long] = {
    case intAndLongPattern(s) => Try(s.toLong).toEither.leftMap(_ => "parse.expected.long")
    case _                    => Left("parse.expected.long")
  }

  def floatParse: Parse[Float] = {
    case floatPattern(s, _) =>
      Right(s.toFloat).flatMap(n => Either.cond(test = !n.isInfinite, right = n, left = "parse.expected.float"))
    case _ => Left("parse.expected.float")
  }

  def stringParse: Parse[String] = Right(_)

  def enumNameParse[E <: Enumeration](enum: E): Parse[E#Value] = o =>
    Try(enum.withName(o)).fold(_ => Left("parse.expected.enum"), Right(_))

}
