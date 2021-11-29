package de.lightningpayments.lib.csvstreams

import cats.Functor

object ReadResult {

  sealed trait ReadResult[+A] extends Product with Serializable

  final case class ReadSuccess[A](value: A) extends ReadResult[A]
  final case class ReadFailure(msg: String) extends Throwable with ReadResult[Nothing]

  implicit val readResultFunctor: Functor[ReadResult] = new Functor[ReadResult[*]] {
    override def map[A, B](fa: ReadResult[A])(f: A => B): ReadResult[B] = fa match {
      case ReadSuccess(v) => ReadSuccess(f(v))
      case f: ReadFailure => f
    }
  }

}
