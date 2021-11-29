package de.lightningpayments.lib.csvstreams

import org.apache.spark.sql.SparkSession

import java.util.UUID

trait SparkTestSupport {

  private val appName: String = s"app_${UUID.randomUUID().toString}"
  private val master: String = "local[*]"
  private val spark: SparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()

  def withSparkSession[A](f: SparkSession => A): A = f(spark)

}
