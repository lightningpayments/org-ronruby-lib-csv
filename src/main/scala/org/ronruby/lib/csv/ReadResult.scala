package org.ronruby.lib.csv

import cats.{Functor, Semigroup}

sealed trait ReadResult[+A]

object ReadResult {

  final case class ReadSuccess[A](value: A) extends ReadResult[A]
  final case class ReadFailure(msg: String) extends ReadResult[Nothing]

  implicit val failureIsSemigroup: Semigroup[ReadFailure] =
    (f1: ReadFailure, f2: ReadFailure) => ReadFailure(f1.msg + ", " + f2.msg)

  implicit val readResultIsFunctor: Functor[ReadResult] = new Functor[ReadResult[*]] {
    override def map[A, B](fa: ReadResult[A])(f: A => B): ReadResult[B] = fa match {
      case ReadSuccess(v) => ReadSuccess(f(v))
      case f: ReadFailure => f
    }
  }
}
