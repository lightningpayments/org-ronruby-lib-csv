package org.ronruby.lib.csv

import au.com.bytecode.opencsv.CSVReader

object CsvReaderUtil {

  implicit final class RichCSVReader(val reader: CSVReader) extends AnyVal {
    @scala.annotation.tailrec
    def next: Option[Array[String]] =
      Option(reader.readNext) match {
        case Some(line) if isEmpty(line) => next
        case s @ Some(_) => s
        case None => None
      }

    private def isEmpty(line: Array[String]): Boolean =
      line.isEmpty || line.length == 1 && line(0).trim == ""
  }

}
