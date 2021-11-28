package de.lightningpayments.lib.csvstreams

import ch.qos.logback.classic.LoggerContext
import org.mockito.Mockito
import org.mockito.scalatest.MockitoSugar
import org.mockito.stubbing.Stubber
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatestplus.play.PlaySpec
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.Try

class TestSpec extends PlaySpec with MockitoSugar with ScalaFutures with BeforeAndAfterEach with BeforeAndAfterAll {

  import zio.Task

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

  /**
   * The execution context used in unit tests.
   */
  implicit val defaultExecutionContext: ExecutionContext = ExecutionContext.Implicits.global

  /**
   * Runtime default used only in unit tests.
   */
  implicit val zioRuntime: zio.Runtime[zio.ZEnv] = zio.Runtime.default

  /**
   * To prevent tests to fail because of future timeout issues, we override the default behavior.
   * Note: it is an implicit Option for the ScalaFutures method 'whenReady'
   */
  implicit val defaultPatience: PatienceConfig = PatienceConfig(
    timeout = Span(futurePatienceTimeout, Seconds),
    interval = Span(futurePatienceInterval, Millis)
  )

  /**
   * Wrapper method for mockito s doReturn method.
   * Resolves "ambiguous reference to overloaded definition" error
   */
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }

  /**
   * @inheritdoc
   */
  override def beforeAll(): Unit = {
    try super.beforeAll()
    finally {
      // shutdown logger
      Try(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]).map { ctx =>
        ctx.stop()
        org.slf4j.bridge.SLF4JBridgeHandler.uninstall()
      }
    }
  }

  def whenReady[T, U](io: Task[T])(f: Either[Throwable, T] => U): U = zioRuntime.unsafeRun(io.either.map(f))

}
