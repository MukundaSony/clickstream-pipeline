package com.deloai.clickstream.service

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import com.deloai.clickstream.domain.Models._
import org.slf4j.LoggerFactory

class ClickstreamTransformer(implicit spark: SparkSession) {
  import spark.implicits._
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Cleans raw JSON clickstream data by filtering bad records,
    * bot traffic, and casting types appropriately.
    */
  def cleanAndFormat(rawDF: DataFrame): Dataset[CleanClickstreamEvent] = {
    logger.info("Starting data cleaning and formatting...")
    
    val botRegex = "(?i)(bot|crawler|spider)"

    rawDF
      .filter(col("user_agent").isNotNull && !col("user_agent").rlike(botRegex))
      .filter(col("user_id").isNotNull && col("session_id").isNotNull)
      .filter(col("event_timestamp").isNotNull)
      .withColumn("eventTimestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
      .select(
        col("user_id").alias("userId"),
        col("session_id").alias("sessionId"),
        coalesce(col("event_type"), lit("unknown")).alias("eventType"),
        col("product_id").alias("productId"),
        col("user_agent").alias("userAgent"),
        col("eventTimestamp"),
        coalesce(col("time_spent_seconds"), lit(0.0)).alias("timeSpentSeconds")
      )
      .as[CleanClickstreamEvent]
  }

  /**
    * Aggregates cleaned data to generate daily product view analytics.
    */
  def aggregateProductViews(cleanDS: Dataset[CleanClickstreamEvent]): Dataset[ProductViewSummary] = {
    logger.info("Starting product view aggregation...")
    
    cleanDS
      .filter($"eventType" === "view_item" && $"productId".isNotNull)
      .groupBy($"productId")
      .agg(
        countDistinct($"userId").alias("uniqueViewers"),
        avg($"timeSpentSeconds").alias("avgTimeSpentSec")
      )
      .as[ProductViewSummary]
      .orderBy($"uniqueViewers".desc)
  }
}
