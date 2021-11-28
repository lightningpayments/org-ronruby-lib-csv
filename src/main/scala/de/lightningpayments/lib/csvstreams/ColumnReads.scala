package de.lightningpayments.lib.csvstreams

import de.lightningpayments.lib.csvstreams.ReadResult.{ReadFailure, ReadSuccess}
import org.apache.spark.sql.Row
import play.api.libs.functional.{Applicative, FunctionalCanBuild, Functor, ~}

trait ColumnReads[T] {
  def read(line: Row): ReadResult[T]
}

object ColumnReads {

  implicit def functor(implicit app: Applicative[ColumnReads]): Functor[ColumnReads] = new Functor[ColumnReads] {
    override def fmap[A, B](m: ColumnReads[A], f: A => B): ColumnReads[B] = app.map[A, B](m, f)
  }

  implicit def functionBuildColumnReads(implicit app: Applicative[ColumnReads]): FunctionalCanBuild[ColumnReads] =
    new FunctionalCanBuild[ColumnReads] {
      override def apply[A, B](ca: ColumnReads[A], cb: ColumnReads[B]): ColumnReads[A ~ B] =
        app.apply(app.map[A, B => A ~ B](ca, a => (b: B) => new ~(a, b)), cb)
    }

  implicit val columnReadsApplicative: Applicative[ColumnReads] = new Applicative[ColumnReads] {
    override def pure[A](a: => A): ColumnReads[A] = _ => ReadSuccess(a)

    override def map[A, B](m: ColumnReads[A], f: A => B): ColumnReads[B] = this.apply(pure(f), m)

    override def apply[A, B](ffa: ColumnReads[A => B], fa: ColumnReads[A]): ColumnReads[B] = (row: Row) =>
      (fa.read(row), ffa.read(row)) match {
        case (ReadSuccess(a), ReadSuccess(f)) => ReadSuccess[B](f(a))
        case (f: ReadFailure, _) => f
        case (_, f: ReadFailure) => f
      }

  }

}
