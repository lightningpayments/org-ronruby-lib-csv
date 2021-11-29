package de.lightningpayments.lib.csvstreams

import ch.qos.logback.classic.LoggerContext
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterEach, OptionValues}
import org.slf4j.LoggerFactory
import zio.Task

import scala.language.higherKinds
import scala.util.Try

class TestSpec
  extends AnyWordSpec
  with Matchers
  with OptionValues
  with BeforeAndAfterEach {

  implicit val outerScope: Unit = org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

  override def beforeEach(): Unit = {
    try super.beforeEach()
    finally {
      // shutdown logger
      Try(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]).map { ctx =>
        ctx.stop()
        org.slf4j.bridge.SLF4JBridgeHandler.uninstall()
      }
    }
  }

  /**
   * ZIO default runtime only for testing.
   */
  lazy implicit val runtime: zio.Runtime[zio.ZEnv] = zio.Runtime.default

  def whenReady[T, U](io: => Task[T])(f: Either[Throwable, T] => U): U =
    runtime.unsafeRun(io.either.map(f))

}
