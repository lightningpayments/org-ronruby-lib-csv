package de.lightningpayments.lib.csvstreams

import ch.qos.logback.classic.LoggerContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory
import zio.Task

import java.util.UUID
import scala.language.higherKinds
import scala.util.Try

class SparkTestSpec
  extends AnyWordSpec
  with Matchers
  with BeforeAndAfterEach {

  implicit val outerScope: Unit = org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

  lazy val spark: SparkSession = SparkSession.builder()
    .appName(s"app_${UUID.randomUUID().toString}")
    .master("local[*]")
    .getOrCreate()

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

  def withSparkSession[A](f: SparkSession => A): A = f(spark)

  override def afterEach(): Unit = spark.stop()

}
