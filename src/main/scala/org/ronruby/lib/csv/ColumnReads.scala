package org.ronruby.lib.csv

import cats.implicits._
import ReadResult.{ReadFailure, ReadSuccess}
import play.api.libs.functional.{Applicative, FunctionalCanBuild, Functor, ~}

trait ColumnReads[T] {
  def isHeaderValid(names: Seq[String]): Boolean
  def read(line: Seq[Column]): ReadResult[T]
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

  implicit val columnReadsIsApplicative: Applicative[ColumnReads] = new Applicative[ColumnReads] {
    override def apply[A, B](f: ColumnReads[A => B], fa: ColumnReads[A]): ColumnReads[B] = new ColumnReads[B] {
      override def isHeaderValid(names: Seq[String]): Boolean = fa.isHeaderValid(names) && f.isHeaderValid(names)
      override def read(line: Seq[Column]): ReadResult[B] = (fa.read(line), f.read(line)) match {
        case (ReadSuccess(fav), ReadSuccess(fv)) => ReadSuccess(fv(fav))
        case (f1: ReadFailure, f2: ReadFailure) => f1 |+| f2
        case (f: ReadFailure, _) => f
        case (_, f: ReadFailure) => f
      }
    }
    override def pure[A](a: => A): ColumnReads[A] = new ColumnReads[A] {
      override def isHeaderValid(names: Seq[String]): Boolean = true
      override def read(line: Seq[Column]): ReadResult[A] = ReadSuccess(a)
    }
    override def map[A, B](m: ColumnReads[A], f: A => B): ColumnReads[B] = this.apply(pure(f), m)
  }

}
