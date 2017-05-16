import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.SQLBuilder
import org.codehaus.jettison.json.JSONObject

case class Tweet()

class SparkAnalysis() {

  var session: SparkSession = null

  def formatDate(enDate: String): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    val date: Date = simpleDateFormat.parse(enDate);
    return new SimpleDateFormat("dd.MM").format(date)
  }

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

  def saveToCsv(filePath:String, dataFrame: DataFrame): Unit = {
    dataFrame.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save(filePath)
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

    // Graph 2.a
    val totalTweetParJour1 = spark.sql("" +
      "SELECT candidate as Candidat, Count(Distinct id_str) As totalTweet " +
      "FROM tweets " +
      "WHERE candidate IS NOT NULL AND date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') " +
      "BETWEEN '2017-04-10' AND '2017-04-23' " +
      "GROUP BY candidate " +
      "ORDER BY totalTweet Desc")
      println("Graph 2.a")
    totalTweetParJour1.show(100)

    saveToCsv("./output/graph-2a.csv", totalTweetParJour1)

    // Graph 2.b
    val totalTweetParJour2 = spark.sql("" +
      "SELECT candidate as Candidat, Count(Distinct id_str) As totalTweet " +
      "FROM tweets " +
      "WHERE (candidate = 'macron' OR candidate = 'le pen') " +
      "AND date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') " +
      "BETWEEN '2017-04-24' AND '2017-05-07' " +
      "GROUP BY candidate " +
      "ORDER BY totalTweet DESC")

    println("Graph 2.b")
    totalTweetParJour2.show(100)

    saveToCsv("./output/graph-2b.csv", totalTweetParJour2)

    // Graph 3.a
    val totalSentiParJour1 = spark.sql("" +
      "SELECT date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') AS Jour, candidate as Candidat, Count(Distinct id_str) As nbrJournalier, Round(Avg(sentiment)*10,2) as Sentiment, Count(Distinct id_str) * Round(Avg(sentiment)*10,2) as Score " +
      "FROM tweets " +
      "WHERE candidate IS NOT NULL AND date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') " +
      "BETWEEN '2017-04-10' AND '2017-04-23' " +
      "GROUP BY date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd'), candidate " +
      "ORDER BY Jour ASC, Score DESC")
    println("Graph 3.1")
    totalSentiParJour1.show(1000)

    totalSentiParJour1.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("./output/graph-3.1.csv")

    saveToCsv("./output/graph-3a.csv", totalSentiParJour1)

    //Graph 3.b
    val totalSentiParJour2 = spark.sql("" +
      "SELECT date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') AS Jour, candidate as Candidat, Count(Distinct id_str) As nbrJournalier, Round(Avg(sentiment)*10,2) as Sentiment, Count(Distinct id_str) * Round(Avg(sentiment)*10,2) as Score " +
      "FROM tweets " +
      "WHERE (candidate = 'macron' OR candidate = 'le pen') " +
      "AND date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') " +
      "BETWEEN '2017-04-24' AND '2017-05-07' " +
      "GROUP BY date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd'), candidate " +
      "ORDER BY Jour ASC, Score DESC")
    println("Graph 3.2")
    totalSentiParJour2.show(1000)

    saveToCsv("./output/graph-3b.csv", totalSentiParJour2)

    /*
    val schemaList = totalSentiParJour1.schema.map(_.name).zipWithIndex
    totalSentiParJour1.rdd.map(row =>
      //here rec._1 is column name and rce._2 index
      schemaList.map(rec => (rec._1, row(rec._2))).toMap
    ).collect
    */

 /*
    val totalTweetPolitique = spark.sql("SELECT Count(Distinct id_str) As total FROM tweets WHERE candidate IS NOT NULL ")

    val total = totalTweetPolitique.select("total").first().get(0)

    val resultDFTweetsRatio = spark.sql(s"Select Round((Count(Distinct id_str)/$total)*100,2) as TweetTotal, Round(Avg(sentiment)*10,2) as Sentiment, candidate FROM tweets WHERE candidate IS NOT NULL GROUP By candidate ORDER BY Sentiment DESC")
    resultDFTweetsRatio.show()

    val totalTweetPolitiqueParJour = spark.sql("SELECT date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') as Jour, Count(Distinct id_str) As total FROM tweets WHERE candidate IS NOT NULL GROUP by date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') ORDER BY Jour ASC")
    totalTweetPolitiqueParJour.show()

    val resultDFTweetsRatioPerDate = spark.sql("Select date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') as Jour, candidate as Candidat ,Count(Distinct id_str) as NrbTweets, Round(Avg(sentiment)*10,2) as Sentiment FROM tweets WHERE candidate IS NOT NULL GROUP by candidate, date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') ORDER BY Jour ASC")
    resultDFTweetsRatioPerDate.show()

    /*
     * build charts
     */
    val bc = new ChartBuilder

    // chart 1
    val tstat = new TweetStatistics
    val json = tstat.getData()

    // x-axis represents the dates
    val x:Seq[String] =
      for (i <- 0 to json.length() - 1)
        yield formatDate(json.names().get(i).toString)

    // y1-axis represents not political tweets
    val y1:Seq[Double] =
      for (i <- 0 to json.length() - 1)
        yield new JSONObject(json.get(json.names().get(i).toString).toString).getDouble("notPolitics")

    // y2-axis represents political tweets
    val y2:Seq[Double] =
      for (i <- 0 to json.length() - 1)
        yield new JSONObject(json.get(json.names().get(i).toString).toString).getDouble("politics")

    bc.buildTimeline(x, y1, y2)

    // chart 2
    bc.buildSentiments()
*/
  }

  def stop(): Unit = {
    this.session.stop
  }
}
