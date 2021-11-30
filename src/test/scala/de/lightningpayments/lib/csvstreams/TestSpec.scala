package de.lightningpayments.lib.csvstreams

import ch.qos.logback.classic.LoggerContext
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory
import zio.Task

import scala.language.higherKinds
import scala.util.Try

class TestSpec
  extends AnyWordSpec
  with Matchers
  with BeforeAndAfterEach {

  implicit val outerScope: Unit = org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

  /**
   * ZIO default runtime only for testing.
   */
  lazy implicit val runtime: zio.Runtime[zio.ZEnv] = zio.Runtime.default

  def whenReady[A, B](io: Task[A])(f: Either[Throwable, A] => B): B =
    runtime.unsafeRun(io.either.map(f))

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

}
