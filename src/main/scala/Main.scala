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

  /**
    * Main method
    * @param args
    */
  def main(args: Array[String]): Unit = {
    /*
     * once we collected tweets (in JSON format) thanks to our first part,
     * we put them in input folder in order to select wanted attributes and
     * add some attributes such as the candidate name, the sentiment, and we put the tweet in
     * the correct file according to the date and if it concerns politics.
     * For example: 2017-05-05.json or 2017-05-05.politics.json
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

    /*
      * in order to improve the speed of this step,
      * we use many threads to use the processor efficiently
      */
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
     * after finishing the collect of data,
     * we set statistics (ratio betweet not political and political tweets)
     */
    val ts = new TweetStatistics
    ts.setStats()
    ts.showRatios()

    /*
     * finally, we run Spark which permits to execute queries (in Spark SQL)
     * and output SQL responses in CSV files
     */
    val sa = new SparkAnalysis
    sa.run()
    sa.stop()
  }
}