import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.sql.Timestamp
import java.time.Instant

object generate_test_data {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestDataGenerator")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    val path = "src/test/resources/data/input/date=2026-03-05"
    println(s"Generating test data at $path")

    val schema = StructType(Array(
      StructField("user_id", StringType, true),
      StructField("session_id", StringType, true),
      StructField("event_type", StringType, true),
      StructField("product_id", StringType, true),
      StructField("user_agent", StringType, true),
      StructField("event_timestamp", StringType, true),
      StructField("time_spent_seconds", DoubleType, true)
    ))

    val now = Instant.now().toString

    val data = Seq(
      Row("u1", "s1", "view_item", "p1", "Mozilla/5.0", now, 15.5),
      Row("u2", "s2", "checkout", "p1", "Chrome/90.0", now, 120.0),
      Row("u1", "s1", "add_to_cart", "p2", "Mozilla/5.0", now, 35.0),
      Row(null, "s3", "view_item", "p3", "Mozilla/5.0", now, 10.0), // Missing user_id
      Row("u3", "s4", "view_item", "p1", "Googlebot/2.1", now, 2.0) // Bot traffic
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    df.write.mode("overwrite").json(path)
    
    println("Test data generated.")
    spark.stop()
  }
}
