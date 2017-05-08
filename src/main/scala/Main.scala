/**
  * Main
  */

object Main {

  def main(args: Array[String]): Unit = {

    /*
     * transform will read the input file contained in input/ directory,
     * normalize them and then write in the output/ directory.
     */
    val tn = new TweetNormalizer
    tn.transform("sample-300.json")

    /*
     * run Spark: set data frame and make queries (SQL like)
     */
    val sa = new SparkAnalysis
    sa.runQueries()
    sa.stop()
  }
}