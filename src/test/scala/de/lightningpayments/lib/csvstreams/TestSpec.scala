package de.lightningpayments.lib.csvstreams

import ch.qos.logback.classic.LoggerContext
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterEach, OptionValues}
import org.slf4j.LoggerFactory
import zio.Task

import scala.util.Try

class TestSpec
  extends AnyWordSpec
  with Matchers
  with OptionValues
  with ScalaFutures
  with BeforeAndAfterEach
  with Serializable {

  /**
   * The timeout to wait for the future before declaring it as failed.
   * Note: Seconds
   */
  protected val futurePatienceTimeout: Int = 5

  /**
   * The interval for the checking whether the future has completed
   * Note: Milliseconds
   */
  protected val futurePatienceInterval: Int = 15

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

  def whenReady[T, U](io: => Task[T])(f: Either[Throwable, T] => U): U =
    zio.Runtime.default.unsafeRun(io.either.map(f))

}
