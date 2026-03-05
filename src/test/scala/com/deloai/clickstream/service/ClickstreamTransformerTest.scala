package com.deloai.clickstream.service

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class ClickstreamTransformerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ClickstreamTransformerTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("cleanAndFormat should remove bot traffic and reformat dates") {
    val transformer = new ClickstreamTransformer()
    import spark.implicits._

    val rawData = Seq(
      // Valid Row
      ("""{"user_id": "u1", "session_id": "s1", "event_type": "view_item", "product_id": "p1", "user_agent": "Mozilla/5.0", "event_timestamp": "2026-03-05T10:00:00.000Z", "time_spent_seconds": 120}"""),
      // Bot row
      ("""{"user_id": "u2", "session_id": "s2", "event_type": "view_item", "product_id": "p2", "user_agent": "Googlebot Search", "event_timestamp": "2026-03-05T10:05:00.000Z", "time_spent_seconds": 10}"""),
      // Missing required field (user_id)
      ("""{"session_id": "s3", "event_type": "view_item", "product_id": "p3", "user_agent": "Mozilla/5.0", "event_timestamp": "2026-03-05T10:10:00.000Z"}""")
    )

    val rawDF = spark.read.json(rawData.toDS())
    
    val cleanDS = transformer.cleanAndFormat(rawDF)
    val result = cleanDS.collect()

    result.length shouldBe 1
    val validRecord = result.head
    
    validRecord.userId shouldBe "u1"
    validRecord.userAgent shouldBe "Mozilla/5.0"
    validRecord.timeSpentSeconds shouldBe 120.0
  }

  test("aggregateProductViews should accurately count unique viewers and average time") {
    val transformer = new ClickstreamTransformer()
    import spark.implicits._
    import java.sql.Timestamp

    val cleanData = Seq(
      ("u1", "s1", "view_item", Some("p1"), "Mozilla", new Timestamp(1000), 10.0),
      ("u2", "s2", "view_item", Some("p1"), "Mozilla", new Timestamp(2000), 20.0),
      ("u1", "s3", "view_item", Some("p2"), "Mozilla", new Timestamp(3000), 15.0),
       // Should be ignored as it is not a view_item event
      ("u3", "s4", "add_to_cart", Some("p1"), "Mozilla", new Timestamp(4000), 5.0)
    ).toDF("userId", "sessionId", "eventType", "productId", "userAgent", "eventTimestamp", "timeSpentSeconds")
     .as[com.deloai.clickstream.domain.Models.CleanClickstreamEvent]

    val aggDS = transformer.aggregateProductViews(cleanData)
    val result = aggDS.collect()

    // We should only have summaries for grouped products (p1 and p2)
    result.length shouldBe 2
    
    val p1Summary = result.find(_.productId == "p1").get
    p1Summary.uniqueViewers shouldBe 2 // u1 and u2 viewed it
    p1Summary.avgTimeSpentSec shouldBe 15.0 // (10+20) / 2
    
    val p2Summary = result.find(_.productId == "p2").get
    p2Summary.uniqueViewers shouldBe 1 // only u1 viewed it
  }
}
