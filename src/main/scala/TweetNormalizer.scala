import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.codehaus.jettison.json.JSONObject
import org.json4s.JsonAST.{JInt, JString}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.Json
import scala.util.control.Breaks._

import scala.io.Source

/**
  * Jvalue to String
  */
object Get {
  def string(value: JValue): String = {
    val JString(result) = value
    result
  }

  def int(value: JValue): BigInt = {
    val JInt(result) = value
    result
  }
}

object JsonUtil {
  def hasObject(json: JSONObject, value: String): Boolean = {
    try {
      json.getJSONObject(value)
      true
    } catch {
      case e: Exception => {

      }
        false
    }
  }

  def getNestedObjectValue(json: JSONObject, field: String, key: String): String = {
    try {
      return json.getJSONObject(field).getString(key)
    } catch {
      case e: Exception => {

      }
        return null
    }
  }
}

class TweetNormalizer {

  val candidates = Seq(
    "melenchon",
    "cheminade",
    "le pen",
    "lepen",
    "dupont aignan",
    "arthaud",
    "lassalle",
    "poutou",
    "fillon",
    "macron",
    "hamon",
    "asselineau"
  )

  private def findCandidate(text: String, hashtags: String): String = {
    var candidateFound = ""
    candidates.foreach(c => {
      if (text.toLowerCase.contains(c)) {
        candidateFound = c
      }
    })

    if (candidateFound.eq("")) {
      candidates.foreach(c => {
        if (hashtags.toLowerCase.contains(c)) {
          candidateFound = c
        }
      })
    }

    return candidateFound
  }

  def transform(inputFile: String): Unit = {
    val filename = "./input/" + inputFile

    // Check if input file exists
    if (!(new File(filename).exists())) {
      throw new Exception("Input file does not exist: " + filename);
    }

    val linesLength = Source.fromFile(filename).getLines().length
    var lineNumber = 0
    var lineError = 0

    println("######")
    println("Extracting " + linesLength + " tweets from " + filename)
    println("######")

    var currentLine: String = ""

    for (line <- Source.fromFile(filename).getLines()) {
      breakable {
        currentLine = line
        lineNumber += 1

        // remove comma as first char
        if (currentLine.charAt(0).toString == ",") {
          currentLine = currentLine.substring(1)
        }
        // remove comma as last char
        if (currentLine.charAt(currentLine.length - 1).toString == ",") {
          currentLine = currentLine.substring(0, currentLine.length - 1)
        }

        try {
          val json = new JSONObject(currentLine);
          val tweet = collection.mutable.Map[String, Any]()

          // flat
          tweet("created_at") = json.getString("created_at")
          tweet("id_str") = json.getString("id_str")
          tweet("text") = json.getString("text")
          tweet("retweet_count") = json.getInt("retweet_count")
          tweet("favorite_count") = json.getInt("favorite_count")
          tweet("lang") = json.getString("lang")
          tweet("retweeted") = json.getBoolean("retweeted")
          tweet("favorited") = json.getBoolean("favorited")

          // user
          tweet("user_id") = JsonUtil.getNestedObjectValue(json, "user", "id")
          tweet("user_location") = JsonUtil.getNestedObjectValue(json, "user", "location")
          tweet("user_statuses_count") = JsonUtil.getNestedObjectValue(json, "user", "statuses_count")
          tweet("user_created_at") = JsonUtil.getNestedObjectValue(json, "user", "created_at")
          tweet("user_lang") = JsonUtil.getNestedObjectValue(json, "user", "lang")

          // place
          tweet("country_code") = JsonUtil.getNestedObjectValue(json, "place", "country_code")
          tweet("place_name") = JsonUtil.getNestedObjectValue(json, "place", "name")

          // entities
          var strHashtags = ""
          val hashtags = json.getJSONObject("entities").getJSONArray("hashtags")

          for (i <- 0 to hashtags.length() - 1) {
            strHashtags += hashtags.getJSONObject(i).getString("text") + " "
          }

          tweet("hashtags_str") = strHashtags
          tweet("candidate") = this.findCandidate(tweet("text").toString, tweet("hashtags_str").toString)

          // append in correct file according to the tweet's date
          var write = Json(DefaultFormats).write(tweet).toString

          val s: String = tweet("created_at").toString
          val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
          val date: Date = simpleDateFormat.parse(s);
          val outputFile = "./output/" + new SimpleDateFormat("yyyy-MM-dd").format(date) + ".json"

          // append to new line only if the file contains at least 1 tweet
          if ((new File(outputFile).exists()) && Source.fromFile(outputFile).getLines().length > 0) {
            write = "\n" + write
          }

          val fw = new FileWriter(outputFile, true)
          fw.write(write)
          fw.close()

        } catch {
          case e: Exception => {
            println("WARNING: " + e.getMessage)
          }
            lineError += 1
            break
        }
      }
    }

    // at end
    println(lineError + " tweets have not been extracted (" + ((lineError.toFloat/linesLength.toFloat) * 100) + "% of failure).")
  }
}
