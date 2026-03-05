package com.deloai.clickstream.domain

import java.sql.Timestamp

object Models {
  // Raw Data Structure directly mapped from JSON logs
  case class RawClickstreamEvent(
    user_id: Option[String],
    session_id: Option[String],
    event_type: Option[String],
    product_id: Option[String],
    user_agent: Option[String],
    event_timestamp: Option[String],
    time_spent_seconds: Option[Double]
  )

  // Cleaned Domain Model representing valid data ready for analytics
  case class CleanClickstreamEvent(
    userId: String,
    sessionId: String,
    eventType: String,
    productId: Option[String],
    userAgent: String,
    eventTimestamp: Timestamp,
    timeSpentSeconds: Double
  )

  // Aggregated Analytics Model
  case class ProductViewSummary(
    productId: String,
    uniqueViewers: Long,
    avgTimeSpentSec: Double
  )
}
