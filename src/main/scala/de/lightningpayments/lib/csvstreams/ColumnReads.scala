package de.lightningpayments.lib.csvstreams

import de.lightningpayments.lib.csvstreams.ReadResult.{ReadFailure, ReadSuccess}
import play.api.libs.functional.{Applicative, FunctionalCanBuild, Functor, ~}

trait ColumnReads[T] {
  def read(line: IndexedSeq[String]): ReadResult[T]
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
    override def apply[A, B](f: ColumnReads[A => B], fa: ColumnReads[A]): ColumnReads[B] = (line: IndexedSeq[String]) =>
      (fa.read(line), f.read(line)) match {
        case (ReadSuccess(a), ReadSuccess(f)) => ReadSuccess(f(a))
        case (f: ReadFailure, _) => f
        case (_, f: ReadFailure) => f
      }
    override def pure[A](a: => A): ColumnReads[A] = _ => ReadSuccess(a)
    override def map[A, B](m: ColumnReads[A], f: A => B): ColumnReads[B] = this.apply(pure(f), m)
  }

}
