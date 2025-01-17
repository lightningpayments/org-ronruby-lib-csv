package de.lightningpayments.lib.csvstreams

import cats.Functor
import cats.implicits._

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.UUID
import scala.util.Try

trait Parser[T] {
  def parse(s: String): Either[String, T]
}

object Parser {

  private val intAndLongPattern = "^\\s*(-?\\d+)\\s*$".r

  private val floatPattern = "^\\s*([+-]?([0-9]*[.])?[0-9]+)\\s*$".r

  implicit val functor: Functor[Parser] = new Functor[Parser] {
    override def map[A, B](fa: Parser[A])(f: A => B): Parser[B] = fa.parse(_).map(f(_))
  }

  def parse[T: Parser](s: String): Either[String, T] = implicitly[Parser[T]].parse(s)

  def uuidParse: Parser[UUID] = uuid => Try(UUID.fromString(uuid)).fold(_ => Left("parse.expected.uuid"), Right(_))

  def zonedDateTimeParser(formatter: DateTimeFormatter): Parser[ZonedDateTime] = o =>
    Either
      .fromTry(Try(formatter.parse(o)).map(ZonedDateTime.from))
      .leftMap(_ => "parse.expected.zoneddatetime")

  def localDateTimeParser(formatter: DateTimeFormatter): Parser[LocalDateTime] = o =>
    Either
      .fromTry(Try(formatter.parse(o)).map(LocalDateTime.from))
      .leftMap(_ => "parse.expected.localdatetime")

  def intParse: Parser[Int] = {
    case intAndLongPattern(s) => Try(s.toInt).toEither.leftMap(_ => "parse.expected.int")
    case _                    => Left("parse.expected.int")
  }

  def longParse: Parser[Long] = {
    case intAndLongPattern(s) => Try(s.toLong).toEither.leftMap(_ => "parse.expected.long")
    case _                    => Left("parse.expected.long")
  }

  def floatParse: Parser[Float] = {
    case floatPattern(s, _) =>
      Right(s.toFloat).flatMap(n => Either.cond(test = !n.isInfinite, right = n, left = "parse.expected.float"))
    case _ =>
      Left("parse.expected.float")
  }

  def stringParse: Parser[String] = Right(_)

  def enumNameParse[E <: Enumeration](enum: E): Parser[E#Value] = o =>
    Try(enum.withName(o)).fold(_ => Left("parse.expected.enum"), Right(_))

}
