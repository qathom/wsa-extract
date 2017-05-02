import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, to_date, unix_timestamp}

/**
  * Main
  */

case class Tweet(name: String, likes: BigInt, date: String)

object Main {
  def main(args: Array[String]): Unit = {
    // create the session
    val spark = SparkSession
      .builder()
      .appName("WSA")
      // .config("spark.some.config.option", "some-value")
      .config("spark.master", "local")
      .getOrCreate()

    // create the data set
    val path = "./tweets.json"

    import spark.implicits._

    val tweets = spark.read.json(path).as[Tweet]
      .toDF() // params => rename cols

    tweets.createOrReplaceTempView("tweets")

    // twitter format: EEE MMM dd HH:mm:ss ZZZZZ yyyy
    // example: Fri May 12 12:53:54 +0000 2017
    // format: yyyy-mm-dd
    // tweets.sqlContext.sql("SELECT * from tweets where (date_format(date, '%Y-%m-%d') between '2017-01-01' and '2017-08-08')").show

    val ts = unix_timestamp($"date", "EEE MMM dd HH:mm:ss ZZZZZ yyyy").cast("timestamp")
    tweets.withColumn("ts", ts).show

    // between dates
    val filteredData = tweets.select(tweets("date"),
      date_format(unix_timestamp($"date", "EEE MMM dd HH:mm:ss ZZZZZ yyyy")
        .cast("timestamp"), "yyyy-MM-dd")
        .alias("date_test"))
      .filter($"date_test"
        .between("2015-07-05", "2018-09-02"))

    filteredData.show

    // tweets.show()

    // tweets.filter($"likes" > 20).show

    // tweets.select("name").show()
    // tweets.filter($"likes" > 21).show()
    // tweets.groupBy("age").count().show()

    spark.stop
  }
}