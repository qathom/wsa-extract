import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, to_date, unix_timestamp}
import org.apache.spark.sql.SQLContext
import plotly._, element._, layout._, Plotly._

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

    //tweets.createOrReplaceTempView("tweets")

    // twitter format: EEE MMM dd HH:mm:ss ZZZZZ yyyy
    // example: Fri May 12 12:53:54 +0000 2017
    // format: yyyy-mm-dd
    // tweets.sqlContext.sql("SELECT * from tweets where (date_format(date, '%Y-%m-%d') between '2017-01-01' and '2017-08-08')").show

    val ts = unix_timestamp($"created_at", "EEE MMM dd HH:mm:ss ZZZZZ yyyy").cast("timestamp")
    tweets.withColumn("ts", ts)
    tweets.createOrReplaceTempView("tweets")
    tweets.cache()

    val resultDFTweetsRation = spark.sql("Select Count (Distinct id_str) as TweetTotal, Round(Avg(sentiment)*10,2) as Sentiment, candidate  FROM tweets WHERE candidate is not NULL GROUP By candidate ORDER BY Sentiment DESC")
    resultDFTweetsRation.show()

      /*.show

    // between dates
    val filteredData = tweets.select(tweets("created_at"),
      date_format(unix_timestamp($"created_at", "EEE MMM dd HH:mm:ss ZZZZZ yyyy")
        .cast("timestamp"), "yyyy-MM-dd")
        .alias("date_test"))
        .filter($"date_test"
        .between("2015-04-05", "2018-09-02"))

    filteredData.show

    // tweets.show()

    // tweets.filter($"likes" > 20).show

    // tweets.select("text").show()
    // tweets.filter($"likes" > 21).show()
    // tweets.groupBy("age").count().show()


    // build charts
    val x = 0.0 to 10.0 by 0.1
    val y1 = x.map(d => 2.0 * d + util.Random.nextGaussian())
    val y2 = x.map(math.exp)

    val plot = Seq(
      Scatter(
        x, y1, name = "Approx twice"
      ),
      Scatter(
        x, y2, name = "Exp"
      )
    )

    plot.plot(
      title = "Curves",
      path = "./output/plot.html"
    )
    */
  }

  def stop(): Unit = {
    this.session.stop
  }
}
