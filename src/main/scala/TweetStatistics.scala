import java.io.{File, FileWriter}

import org.codehaus.jettison.json.JSONObject
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.io.Source

class TweetStatistics {

  private val statsFile: String = "./output/stats.json"

  def setStats(): Unit = {
    val ratios = scala.collection.mutable.Map[String, Any]()
    val files = new java.io.File("./output").listFiles.filter(_.getName.endsWith(".politics.json"))
    files.toSeq.foreach(file => {
      val fileDate = file.getName.substring(0, file.getName.indexOf("."))
      val info = scala.collection.mutable.Map[String, Any]()

      info("notPolitics") = Source.fromFile("./output/" + fileDate + ".json").getLines().size
      info("politics") = Source.fromFile("./output/" + fileDate + ".politics.json").getLines().size

      ratios(fileDate.toString) = info
    })

    val fw = new FileWriter(statsFile, false)
    fw.write(Json(DefaultFormats).write(ratios.toMap).toString)
    fw.close()
  }

  def getData(): JSONObject = {
    try {
      return new JSONObject(Source.fromFile(statsFile).getLines().mkString)
    } catch {
      case e: Exception => {
        println("Stats file error: " + e.getLocalizedMessage + " " + e.getCause + " " + e.getMessage)
      }
    }

    return null
  }

  def showRatios(): Unit = {
    val json = getData()

    for (i <- 0 to json.length() - 1) {
      val date = json.names().get(i).toString
      val values = new JSONObject(json.get(date).toString)
      val totalNotPolitics = values.getDouble("notPolitics")
      val totalPolitics = values.getDouble("politics")
      val ratio: Double = totalPolitics / totalNotPolitics

      println("Ratio for: " + date + " = " + ratio)
    }
  }
}
