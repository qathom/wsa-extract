import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.CountDownLatch

import plotly._

/**
  * Main
  */

object Main{

  import java.util.concurrent.ArrayBlockingQueue
  import java.util.concurrent.{Executors,ExecutorService}

  def formatDate(enDate: String): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    val date: Date = simpleDateFormat.parse(enDate);
    return new SimpleDateFormat("dd.MM").format(date)
  }

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
              }finally {
                doneSignal.countDown()
              }
            }
          }
      }

    doneSignal.await()
    val ts = new TweetStatistics
    ts.setStats()
    ts.showRatios()
  }

    /*
     * run Spark: set data frame and make queries (SQL like)
     */
    /*
    val sa = new SparkAnalysis
    sa.run()
    sa.stop()
    */


    /*
     * build charts
     */
    val bc = new ChartBuilder
    bc.build()
  }
}