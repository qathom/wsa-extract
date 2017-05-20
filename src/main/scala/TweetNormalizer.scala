import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.StringUtils
import org.codehaus.jettison.json.JSONObject
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.collection.mutable.HashMap
import scala.util.control.Breaks._
import scala.io.Source

/**
  * Helper to retrieve a value contained in a JSON object
  */
object JsonUtil {
  /**
    * Checks if a key exists in a JSON object
    *
    * @param json
    * @param key
    * @return boolean
    */
  def hasObject(json: JSONObject, key: String): Boolean = {
    try {
      json.getJSONObject(key)
      true
    } catch {
      case e: Exception => {

      }
        false
    }
  }

  /**
    * Retrieves a value from a JSON object
    *
    * @param json
    * @param field
    * @param key
    * @return
    */
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

  /**
    * Map to find the candidate according to the hashtags and mentions
    */
  private val candidates: Map[String, Set[String]] = Map(
    "arthaud" -> Set("n_arthaud", "Arthaud", "LutteOuvrière", "LO"),
    "asselineau" -> Set("UPR_Asselineau", "JeVoteAsselineau"),
    "cheminade" -> Set("JCheminade", "Cheminade2017", "CHEMINADE", "JeVoteCheminade", "JacquesCheminade"),
    "aignan" -> Set("dupontaignan", "DupontAignan", "JeVoteDupontAignan"),
    "fillon" -> Set("FrancoisFillon", "TousFillon", "Fillon2017_fr", "Fillon", "Fillon2017", "FF2017", "FillonGate"),
    "hamon" -> Set("benoithamon", "Hamon2017", "Hamon2022"),
    "lassalle" -> Set("jeanlassalle", "JeVoteLassalle"),
    "le_pen" -> Set("MLP_officiel", "Marine2017", "LePen", "MLP", "MLPTF1"),
    "macron" -> Set("EmmanuelMacron", "Macron", "EM", "EnMarche", "JeVoteMacron", "Macron2017", "MacronLeak", "MacronGate"),
    "melenchon" -> Set("JLMelenchon‏", "Melenchon", "JLM"),
    "poutou" -> Set("PhilippePoutou", "JeVotePoutou", "Poutou2017", "NPA")
  )

  /**
    * Map to figure out if a tweet concerns politics
    */
  private val presidentialElections: Set[String] = Set(
    "Presidentielle",
    "PCF",
    "Groupe_PRG_CI",
    "PRG",
    "GauchePS",
    "PS",
    "MoDem",
    "LiberalUMP",
    "UMP",
    "MPF",
    "FN_officiel",
    "FN",
    "FNGate",
    "lesRépublicains",
    "LR"
  )

  /**
    * Tweet sentiment in French
    */
  private val TweetFrSent = new TweetFrSentiment

  /**
    * Retrieves the candidate name contained in a text
    *
    * @param text
    * @return string
    */
  private def getCandidate(text: String): String = {
    val words = text.replace("#", "").replace("@", "").toLowerCase.replace("le pen", "le_pen").replace("lepen", "le_pen").split(" +").toSeq.map(w => StringUtils.stripAccents(w))
    val candidatesFound = candidates.map(c => (c._1.toLowerCase, c._2.map(w => w.toLowerCase()))).filter(c => words.exists(w => c._1 == w || c._2.contains(w))).keys.toList.distinct

    if (candidatesFound.size != 1) {
      // ignore tweet
      return null
    }

    return candidatesFound.head
  }

  /**
    * Checks if a text concerns politics
    *
    * @param text
    * @return boolean
    */
  private def concernsPolitics(text: String): Boolean = {
    val words = text.replace("#", "").replace("@", "").toLowerCase.replace("le pen", "le_pen").replace("lepen", "le_pen").split(" +").toSeq.map(w => StringUtils.stripAccents(w).toLowerCase)
    return words.exists(w => presidentialElections.map(p => p.toLowerCase).contains(w)) || words.exists(w => candidates.map(c => (c._1.toLowerCase, c._2.map(w => w.toLowerCase()))).exists(c => c._1 == w || c._2.contains(w)))
  }

  /**
    * Returns the correct filename according to the tweet's date
    * and if it concerns politics (.politics.json)
    *
    * @param inputFileName
    * @param createdAt
    * @param isPolitical
    * @return
    */
  private def getOutputFile(inputFileName: String, createdAt: String, isPolitical: Boolean): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
    val date: Date = simpleDateFormat.parse(createdAt);
    val formatDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
    val outputFilename = "./output/" + formatDate


    if (isPolitical) {
      return outputFilename + ".politics.json"
    } else {
      return outputFilename + ".json"
    }
  }

  /**
    * Reads JSON files contained in input directory, adds attributes such as
    * the candidate name and the tweet sentiment. Finally, we write the tweet
    * in a file according to the date of the tweet and if it concerns politics or not
    *
    * @param inputFile
    */
  def transform(inputFile: String): Unit = {
    val filename = "./input/" + inputFile
    var stats: scala.collection.mutable.HashMap[String, Seq[Int]] = HashMap()

    // Check if input file exists
    if (!(new File(filename).exists())) {
      throw new Exception("Input file does not exist: " + filename);
    }

    val source = Source.fromFile(filename)
    var lineNumber = 0
    var lineError = 0
    val linesSize = source.getLines().size
    source.close()

    println("######")
    println("Extracting " + linesSize + " Twitter API messages from " + filename)

    var currentLine: String = ""

    val source2 = Source.fromFile(filename)

    for (line <- source2.getLines()) {
      var isPolitical = false
      breakable {
        currentLine = line
        lineNumber += 1

        // remove comma as first char
        if (currentLine.length > 0 && currentLine.charAt(0).toString == ",") {
          currentLine = currentLine.substring(1)
        }

        // remove comma as last char
        if (currentLine.length > 0 && currentLine.charAt(currentLine.length - 1).toString == ",") {
          currentLine = currentLine.substring(0, currentLine.length - 1)
        }

        try {
          val json = new JSONObject(currentLine);
          val tweet = collection.mutable.Map[String, Any]()

          // add primary data
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

          tweet("created_at") = json.getString("created_at")
          tweet("id_str") = json.getString("id_str")

          // only if it concerns politics, we add the candidate name and the sentiment
          if (this.concernsPolitics(json.getString("text").toString())) {
            isPolitical = true
            tweet("candidate") = getCandidate(json.getString("text").toString())
            tweet("sentiment") = TweetFrSent.getSentiment(json.getString("text").toString())
          }

          // append in correct file according to the tweet's date
          var write = Json(DefaultFormats).write(tweet).toString
          val outputFile = this.getOutputFile(inputFile, tweet("created_at").toString, isPolitical)


          // append to new line only if the file contains at least 1 tweet
          if ((new File(outputFile).exists())) {
            val source3 = Source.fromFile(outputFile)
            if(source3.getLines().length > 0) {
              write = "\n" + write
            }
            source3.close()
          }

          val fw = new FileWriter(outputFile, true)
          try {
            fw.write(write)
          } finally {
            fw.close()
          }
        } catch {
          case e: Exception => {
            println("WARNING: " + e.getLocalizedMessage + " " + e.getCause + " " + e.getMessage)
            lineError += 1
            break
          }
        }
      }
    }

    // finally we show statistics
    println(lineNumber + " " + "lines have been read.")
    println(lineError + " " + " lines ignored (" + ((lineError.toFloat / linesSize.toFloat) * 100) + "% of failure).")

    source2.close()
  }
}
