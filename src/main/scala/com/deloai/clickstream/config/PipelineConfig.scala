package com.deloai.clickstream.config

import pureconfig._
import pureconfig.generic.auto._

case class AppConfig(environment: String, name: String)
case class SparkConfig(master: String, appName: String, properties: Map[String, String])
case class HDFSConfig(uri: String, inputBasePath: String, outputCleanPath: String, outputAggPath: String)

case class PipelineConfig(app: AppConfig, spark: SparkConfig, hdfs: HDFSConfig)

object ConfigLoader {
  def load(): PipelineConfig = {
    ConfigSource.default.loadOrThrow[PipelineConfig]
  }
}
