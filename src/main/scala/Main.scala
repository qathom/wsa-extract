import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
  * Main
  */
object Main {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  private def formatDate(enDate: String): String = {
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
    for (f <- inputFiles) {
      q.put(f.getName)
    }
    java.util.Collections.shuffle(java.util.Arrays.asList(inputFiles))
    val threadNumbers = Runtime.getRuntime.availableProcessors
    val doneSignal: CountDownLatch = new CountDownLatch(threadNumbers)
    val pool: ExecutorService = Executors.newFixedThreadPool(threadNumbers)
    println(threadNumbers + " " + "threads will be used.")
    println("Preparing " + numberOfFiles + " files")

    try {
      1 to threadNumbers foreach { x =>
        pool.execute(
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
        )
      }
    } finally {
      pool.shutdown()
    }

    doneSignal.await()

    /*
     * add tweet statistics such as the number
     * of political and no-political tweets for each day
     */
    val ts = new TweetStatistics
    ts.setStats()
    ts.showRatios()

    // build the chart based on the JSON file (stats.json)
    val bc = new ChartBuilder
    val list = ts.getDataListSorted()

    // x-axis represents the dates
    val x:Seq[String] =
      for (el <- list)
        yield formatDate(el._1)

    // y1-axis represents not political tweets
    val y1:Seq[Double] =
      for (el <- list)
        yield el._2.get("notPolitics").get

    // y2-axis represents political tweets
    val y2:Seq[Double] =
      for (el <- list)
        yield el._2.get("politics").get

    bc.buildTimeline(x, y1, y2)

    /*
     * run Spark: set data frame, make queries (SQL like) and output data in CSV files
     */
    val sa = new SparkAnalysis
    sa.run()
    sa.stop()
  }
}