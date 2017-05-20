import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Simple case class
  */
case class Tweet()

class SparkAnalysis() {

  var session: SparkSession = null

  /**
    * Creates a Spark session
    *
    * @return SparkSession
    */
  private def createSession(): SparkSession = {
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

  /**
    * Saves data in CSV format
    *
    * @param filePath
    * @param dataFrame
    */
  private def saveToCsv(filePath:String, dataFrame: DataFrame): Unit = {
    dataFrame.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save(filePath)
  }

  /**
    * Runs a Spark Session and executes Spark SQL requests
    */
  def run(): Unit = {
    val spark = this.createSession()

    import spark.implicits._

    // we analyze political tweets only, so the wildcard is *.politics.json
    val tweets = spark.read.json("./output/*.politics.json").as[Tweet]
      .toDF() // we don't add parameters because we want to keep the same column names
      .cache()

    tweets.createOrReplaceTempView("tweets")

    // first query
    val totalTweetParJour1 = spark.sql("" +
      "SELECT candidate as Candidat, Count(Distinct id_str) As totalTweet " +
      "FROM tweets " +
      "WHERE candidate IS NOT NULL AND date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') " +
      "BETWEEN '2017-04-10' AND '2017-04-23' " +
      "GROUP BY candidate " +
      "ORDER BY totalTweet Desc")

    saveToCsv("./output/graph-2a.csv", totalTweetParJour1)

    // second query
    val totalTweetParJour2 = spark.sql("" +
      "SELECT candidate as Candidat, Count(Distinct id_str) As totalTweet " +
      "FROM tweets " +
      "WHERE (candidate = 'macron' OR candidate = 'le pen') " +
      "AND date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') " +
      "BETWEEN '2017-04-24' AND '2017-05-07' " +
      "GROUP BY candidate " +
      "ORDER BY totalTweet DESC")

    saveToCsv("./output/graph-2b.csv", totalTweetParJour2)

    // third query
    val totalSentiParJour1 = spark.sql("" +
      "SELECT date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') AS Jour, candidate as Candidat, Count(Distinct id_str) As nbrJournalier, Round(Avg(sentiment)*10,2) as Sentiment, Count(Distinct id_str) * Round(Avg(sentiment)*10,2) as Score " +
      "FROM tweets " +
      "WHERE candidate IS NOT NULL AND date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') " +
      "BETWEEN '2017-04-10' AND '2017-04-23' " +
      "GROUP BY date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd'), candidate " +
      "ORDER BY Jour ASC, Score DESC")

    saveToCsv("./output/graph-3a.csv", totalSentiParJour1)

    // fourth query
    val totalSentiParJour2 = spark.sql("" +
      "SELECT date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') AS Jour, candidate as Candidat, Count(Distinct id_str) As nbrJournalier, Round(Avg(sentiment)*10,2) as Sentiment, Count(Distinct id_str) * Round(Avg(sentiment)*10,2) as Score " +
      "FROM tweets " +
      "WHERE (candidate = 'macron' OR candidate = 'le pen') " +
      "AND date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd') " +
      "BETWEEN '2017-04-24' AND '2017-05-07' " +
      "GROUP BY date_format(cast(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') AS TIMESTAMP), 'yyyy-MM-dd'), candidate " +
      "ORDER BY Jour ASC, Score DESC")

    saveToCsv("./output/graph-3b.csv", totalSentiParJour2)
  }

  /**
    * Stops the Spark session
    */
  def stop(): Unit = {
    this.session.stop
  }
}
