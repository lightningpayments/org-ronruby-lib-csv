package org.ronruby.lib.csv

import java.io.StringReader

import org.ronruby.lib.csv.errors.FailureThrowable
import CsvReaderUtil._
import ReadResult.{ReadFailure, ReadSuccess}
import au.com.bytecode.opencsv.CSVReader
import zio.Task

trait CsvParser {

  def isHeaderValidWithSeparator[T](
    str: String,
    separator: Char)(
    implicit cr: ColumnReads[T]
  ): Task[Boolean] =
    readerAndHeader(str, separator).map { case (_, header) => cr.isHeaderValid(header.toIndexedSeq) }

  def parseWithSeparator[T](
    str: String,
    separator: Char)(
    implicit cr: ColumnReads[T]
  ): Task[List[T]] =
    readerAndHeader(str, separator).flatMap {
      case (reader, header) =>
        val parseLine: Array[String] => ReadResult[T] = line =>
          cr.read(line = header.toIndexedSeq.zip(line).map { case (c, h) => Column(c, h) })
        @scala.annotation.tailrec def read(lineNum: Int, acc: Seq[T]): Either[FailureThrowable, Seq[T]] =
          reader.next match {
            case Some(line) => parseLine(line) match {
              case ReadSuccess(t) => read(lineNum + 1, acc :+ t)
              case ReadFailure(msg) => Left[FailureThrowable, Seq[T]](FailureThrowable(lineNum, line.mkString(","), msg))
            }
            case None => Right[FailureThrowable, Seq[T]](acc)
          }
        Task.fromEither(read(0, Nil).map(_.to(List)))
    }

  private def readerAndHeader(str: String, separator: Char): Task[(CSVReader, Array[String])] = Task {
    val reader = new CSVReader(new StringReader(str), separator)
    val header = reader.next.getOrElse(Array.empty)
    (reader, header)
  }

  def write[T <: Product](header: List[String], separator: Char, fa: List[T]): String =
    (header.mkString(separator.toString) :: fa.map(_.toCSV(separator.toString))).mkString("\n").trim

  implicit class RichCSVParser[T <: Product](prod: T) {
    def toCSV(separator: String): String = prod.productIterator.map {
      case Some(value) => value
      case None => ""
      case rest => rest
    }.mkString(separator)
  }

}

object CsvParser extends CsvParser
