/**
  * Main
  */

object Main {

  def main(args: Array[String]): Unit = {
    /*
     * transform will read the input file contained in input/ directory,
     * normalize them and then write in the output/ directory.
     */
    // val tn = new TweetNormalizer
    // tn.transform("sample-300.json")
    // val queue = new java.io.File("./input").listFiles.filter(_.getName.endsWith(".json"))

    val thread1 = new Thread {
      override def run: Unit = {
        val tn = new TweetNormalizer
        tn.transform("sample-300.json")
      }
    }
    thread1.start

    val thread2 = new Thread {
      override def run: Unit = {
        val tn = new TweetNormalizer
        tn.transform("sample-300.json")
      }
    }
    thread2.start

    /**
      * Show statistics from output such as
      * the ratio of political tweets
      */
      /*
    val ts = new TweetStatistics
    ts.setStats()
    ts.showRatios()
    */

    /*
     * run Spark: set data frame and make queries (SQL like)
     */
    /*
    val sa = new SparkAnalysis
    sa.run()
    sa.stop()
    */
  }
}