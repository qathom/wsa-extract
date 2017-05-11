/**
  * Main
  */

object Main{

  import java.util.concurrent.ArrayBlockingQueue
  import java.util.concurrent.{Executors,ExecutorService}

  def main(args: Array[String]): Unit = {
    /*
     * transform will read the input file contained in input/ directory,
     * normalize them and then write in the output/ directory.
     */

    val threadNumbers = Runtime.getRuntime.availableProcessors
    val inputFiles = new java.io.File("./input").listFiles.filter(_.getName.endsWith(".json"))
    val numberOfFiles = inputFiles.size
    val q = new ArrayBlockingQueue[String](numberOfFiles, true)
    for (f <- inputFiles) {
      q.put(f.getName)
    }

    val pool: ExecutorService = Executors.newFixedThreadPool(threadNumbers)

    try {
      1 to threadNumbers foreach { x =>
        pool.execute(
          new Runnable {
            def run {
              val tn = new TweetNormalizer
              while (q.size() > 0) {
                val fileName = q.take()
                println("Processing file " + fileName)
                tn.transform(fileName)
              }
            }
          }
        )
      }
    } finally {
      pool.shutdown()
      val ts = new TweetStatistics
      ts.setStats()
      ts.showRatios()
    }
  }

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