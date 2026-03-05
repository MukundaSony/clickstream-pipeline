package com.deloai.clickstream.jobs

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import com.deloai.clickstream.config.ConfigLoader
import com.deloai.clickstream.service.ClickstreamTransformer

object ClickstreamETLJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val processDate = if (args.length > 0) args(0) else {
      logger.error("Usage: ClickstreamETLJob <yyyy-MM-dd>")
      sys.exit(1)
    }

    val config = ConfigLoader.load()
    
    implicit val spark: SparkSession = SparkSession.builder()
      .appName(config.spark.appName)
      .master(config.spark.master)
      .config(config.spark.properties.foldLeft(new org.apache.spark.SparkConf()) {
        case (conf, (k, v)) => conf.set(k, v)
      })
      .getOrCreate()

    try {
      val transformer = new ClickstreamTransformer()

      // 1. Extract
      val inputPath = s"${config.hdfs.uri}${config.hdfs.inputBasePath}/date=$processDate"
      logger.info(s"Reading raw data from: $inputPath")
      val rawDF = spark.read.json(inputPath)

      // 2. Transform (Clean)
      val cleanDS = transformer.cleanAndFormat(rawDF)
      cleanDS.cache()

      // 3. Load Clean Data
      val cleanOutputPath = s"${config.hdfs.uri}${config.hdfs.outputCleanPath}/date=$processDate"
      logger.info(s"Writing clean data to: $cleanOutputPath")
      cleanDS.write.mode("overwrite").parquet(cleanOutputPath)

      // 4. Transform (Aggregate)
      val analyticsDS = transformer.aggregateProductViews(cleanDS)

      // 5. Load Analytics Data
      val analyticsOutputPath = s"${config.hdfs.uri}${config.hdfs.outputAggPath}/date=$processDate"
      logger.info(s"Writing analytics data to: $analyticsOutputPath")
      analyticsDS.write.mode("overwrite").option("header", "true").csv(analyticsOutputPath)

      logger.info(s"ETL Pipeline completed successfully for date $processDate")

    } catch {
      case e: Exception =>
        logger.error("Pipeline failed", e)
        throw e
    } finally {
      spark.stop()
    }
  }
}
