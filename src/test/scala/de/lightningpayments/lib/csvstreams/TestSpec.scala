package de.lightningpayments.lib.csvstreams

import ch.qos.logback.classic.LoggerContext
import org.apache.log4j.{Logger => Log4jLogger}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory
import zio.Task

import scala.language.higherKinds
import scala.util.Try

abstract class TestSpec extends AnyWordSpec with Matchers with SparkTestSupport { self =>

  implicit val outerScope: Unit = org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(self)

  implicit val logger: Log4jLogger = Log4jLogger.getLogger(this.getClass)

  /**
   * ZIO default runtime only for testing.
   */
  lazy implicit val runtime: zio.Runtime[zio.ZEnv] = zio.Runtime.default

  def whenReady[A, B](io: Task[A])(f: Either[Throwable, A] => B): B =
    runtime.unsafeRun(io.either.map(f))

  Try(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]).map { ctx =>
    ctx.stop()
    org.slf4j.bridge.SLF4JBridgeHandler.uninstall()
  }

}
