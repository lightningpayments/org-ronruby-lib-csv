package org.ronruby.lib.csv.errors

final case class FailureThrowable(lineNum: Int, line: String, message: String) extends Throwable
