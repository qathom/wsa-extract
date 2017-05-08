import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, to_date, unix_timestamp}

case class Tweet()

class SparkAnalysis() {

  var session: SparkSession = null

  def createSession(): SparkSession = {
    val session = SparkSession
      .builder()
      .appName("WSA")
      // .config("spark.some.config.option", "some-value")
      .config("spark.master", "local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    this.session = session

    return session

  }

  def runQueries(): Unit = {
    val spark = this.createSession()

    import spark.implicits._


    val tweets = spark.read.json("./output/*.json").as[Tweet]
      .toDF() // params => rename cols
      .cache()

    tweets.createOrReplaceTempView("tweets")

    // twitter format: EEE MMM dd HH:mm:ss ZZZZZ yyyy
    // example: Fri May 12 12:53:54 +0000 2017
    // format: yyyy-mm-dd
    // tweets.sqlContext.sql("SELECT * from tweets where (date_format(date, '%Y-%m-%d') between '2017-01-01' and '2017-08-08')").show

    val ts = unix_timestamp($"created_at", "EEE MMM dd HH:mm:ss ZZZZZ yyyy").cast("timestamp")
    tweets.withColumn("ts", ts).show

    // between dates
    val filteredData = tweets.select(tweets("created_at"),
      date_format(unix_timestamp($"created_at", "EEE MMM dd HH:mm:ss ZZZZZ yyyy")
        .cast("timestamp"), "yyyy-MM-dd")
        .alias("date_test"))
        .filter($"date_test"
        .between("2015-07-05", "2018-09-02"))

    filteredData.show

    // tweets.show()

    // tweets.filter($"likes" > 20).show

    // tweets.select("text").show()
    // tweets.filter($"likes" > 21).show()
    // tweets.groupBy("age").count().show()
  }

  def stop(): Unit = {
    this.session.stop
  }
}
