/**
  * Main
  */

object Main   {

  def main(args: Array[String]): Unit = {
    /*
     * transform will read the input file contained in input/ directory,
     * normalize them and then write in the output/ directory.
     */
    val tn = new TweetNormalizer
    tn.transform("tweets.json")

    /**
      * Show statistics from output such as
      * the ratio of political tweets
      */
    val ts = new TweetStatistics
    ts.setStats()
    ts.showRatios()

    /*
     * run Spark: set data frame and make queries (SQL like)
     */
    val sa = new SparkAnalysis
    sa.run()
    sa.stop()
  }
}