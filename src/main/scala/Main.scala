import java.util.concurrent.CountDownLatch
import java.util.concurrent.ArrayBlockingQueue

/**
  * Main
  */
object Main {

  def main(args: Array[String]): Unit = {
    /*
     * transform will read the input file contained in input/ directory,
     * normalize them and then write in the output/ directory.
     */

    val inputFiles = new java.io.File("./input").listFiles.filter(_.getName.endsWith(".json"))
    val numberOfFiles = inputFiles.size
    val q = new ArrayBlockingQueue[String](numberOfFiles, true)
    for (f <- inputFiles) {q.put(f.getName)}
    java.util.Collections.shuffle(java.util.Arrays.asList(inputFiles))
    val threadNumbers = Runtime.getRuntime.availableProcessors
    val doneSignal: CountDownLatch = new CountDownLatch(threadNumbers)

    println(threadNumbers + " " + "threads will be used.")
    println("Preparing " + numberOfFiles + " files")

    1 to threadNumbers foreach { x =>
      new Runnable {
        def run {
          try {
            val tn = new TweetNormalizer
            while (q.size() > 0) {
              val fileName = q.take()
              println("Processing file " + fileName)
              //println(pool + "threads will be used.")
              tn.transform(fileName)
            }
          } catch {
            case e: Exception => {
              println("Error: " + e.getLocalizedMessage + " " + e.getCause + " " + e.getMessage)
            }
          } finally {
            doneSignal.countDown()
          }
        }
      }
    }

    doneSignal.await()

    /*
     * add tweet statistics such as the number
     * of political and no-political tweets for each day
     */
    val ts = new TweetStatistics
    ts.setStats()
    ts.showRatios()

    /*
     * run Spark: set data frame, make queries (SQL like) and build charts
     */
    val sa = new SparkAnalysis
    sa.run()
    sa.stop()
  }
}