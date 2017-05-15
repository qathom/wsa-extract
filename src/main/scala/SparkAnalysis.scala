import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, to_date, unix_timestamp}
import org.apache.spark.sql.SQLContext

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
      .config("spark.executor.cores","2")
      .getOrCreate()

    this.session = session

    return session

  }

  def run(): Unit = {
    val spark = this.createSession()

    import spark.implicits._

    val tweets = spark.read.json("./output/*.politics.json").as[Tweet]
      .toDF() // params => rename cols
      .cache()

    tweets.createOrReplaceTempView("tweets")

    // twitter format: EEE MMM dd HH:mm:ss ZZZZZ yyyy
    // example: Fri May 12 12:53:54 +0000 2017
    // format: yyyy-mm-dd
    //tweets.sqlContext.sql("SELECT date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') as Jour FROM tweets").show

    //val ts = unix_timestamp($"created_at", "EEE MMM dd HH:mm:ss ZZZZZ yyyy").cast("timestamp")
    //tweets.withColumn("ts", ts)
   // tweets.createOrReplaceTempView("tweets")
    //tweets.cache()
    //tweets.show()

    val totalTweetPolitique = spark.sql("SELECT Count(Distinct id_str) As total FROM tweets WHERE candidate IS NOT NULL")
    val total = totalTweetPolitique.select("total").first().get(0)

    val resultDFTweetsRatio = spark.sql(s"Select Round((Count(Distinct id_str)/$total)*100,2) as TweetTotal, Round(Avg(sentiment)*10,2) as Sentiment, candidate FROM tweets WHERE candidate IS NOT NULL GROUP By candidate ORDER BY Sentiment DESC")
    resultDFTweetsRatio.show()

    val totalTweetPolitiqueParJour = spark.sql("SELECT date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') as Jour, Count(Distinct id_str) As total FROM tweets WHERE candidate IS NOT NULL GROUP by date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') ORDER BY Jour ASC")

    val resultDFTweetsRatioPerDate = spark.sql("Select date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') as Jour, candidate as Candidat ,Count(Distinct id_str) as NrbTweets FROM tweets WHERE candidate IS NOT NULL GROUP by candidate, date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') ORDER BY Jour ASC")
    resultDFTweetsRatioPerDate.show()

    /*
     * build charts
     */
    val bc = new ChartBuilder

    // chart 1
    bc.buildTimeline()

    // chart 2
    bc.buildSentiments()
  }

  def stop(): Unit = {
    this.session.stop
  }
}
