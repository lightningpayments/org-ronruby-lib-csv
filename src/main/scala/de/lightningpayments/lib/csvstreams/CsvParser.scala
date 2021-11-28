package de.lightningpayments.lib.csvstreams

import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.alpakka.csv.scaladsl.CsvParsing.DoubleQuote
import akka.stream.scaladsl.Source
import akka.util.ByteString
import de.lightningpayments.lib.csvstreams.ReadResult.{ReadFailure, ReadSuccess}
import play.api.Logging

trait CsvParser extends Logging {

  /**
   * If you want to obtain a CSV without losses, you must use this method.
   * However, the whole process is aborted in case of errors.
   */
  def parseStream[T](
    csv: Source[ByteString, _],
    separator: Byte,
    dropLeadingLines: Int = 1,
    ignoreQuoteChar: Option[Byte] = None)(
    implicit cr: ColumnReads[T]
  ): Source[T, _] =
    csv
      .via(CsvParsing.lineScanner(separator, quoteChar = ignoreQuoteChar.getOrElse(DoubleQuote)))
      .drop(dropLeadingLines)
      .map(_.map(_.utf8String))
      .map(line => cr.read(line.toIndexedSeq))
      .flatMapConcat {
        case ReadFailure(msg) => Source.failed(ReadFailure(msg))
        case ReadSuccess(t) => Source.single(t)
      }

  /**
   * If you want to get a CSV and can live with losses, e.g. parsing errors, then unreadable lines are skipped.
   *
   * Double quotes cause a newline within the line string in the stream.
   * Therefore, the LineScanner must know which types may be ignored.
   */
  def parseStreamIgnoringFailure[T](
    csv: Source[ByteString, _],
    separator: Byte,
    dropLeadingLines: Int = 1,
    ignoreQuoteChar: Option[Byte] = None)(
    implicit cr: ColumnReads[T]
  ): Source[T, _] =
    csv
      .via(CsvParsing.lineScanner(separator, quoteChar = ignoreQuoteChar.getOrElse(DoubleQuote)))
      .drop(dropLeadingLines)
      .map(_.map(_.utf8String))
      .map(line => cr.read(line.toIndexedSeq))
      .flatMapConcat {
        case ReadFailure(_) => Source.empty[T]
        case ReadSuccess(t) => Source.single(t)
      }

}

object CsvParser extends CsvParser
