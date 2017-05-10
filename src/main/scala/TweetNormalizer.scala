import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.StringUtils
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

  /*
  private val candidates = Seq(
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
  */

  private val TweetFrSent = new TweetFrSentiment

  val datePol = new scala.collection.mutable.HashMap[Date, Int]()
  val dateNonPol = new scala.collection.mutable.HashMap[Date, Int]()

  val hmCandidat = new scala.collection.mutable.HashMap[String, String]()
  hmCandidat.put("@benoithamon", "Hamon")
  // Remplir le Hashmap


  val hashTagExp = """\B#\w*[a-zA-Z]+\w*""".r
  val mentionExp = """\B@\w*[a-zA-Z]+\w*""".r

  private def getCandidate(text: String): String = {
    var candidateFound: String = null
    var candidateFoundTotal = 0

    val hashtag = hashTagExp.findAllIn(StringUtils.stripAccents(text).toLowerCase).toSet
    val mention = mentionExp.findAllIn(StringUtils.stripAccents(text).toLowerCase).toSet

    val hotWords = hashtag ++ mention


    hotWords.foreach(hw => {

      if (hmCandidat.contains(hw)){

        if (hmCandidat(hw) == candidateFound || candidateFound == null) {
          candidateFound = hmCandidat(hw)
        }else{
          candidateFound = null
          break
        }
      }
    })

    return candidateFound
  }


  val polWords = Seq("#Presidentielle")
  // Remplir la Seq

  private def isPolitique(text: String): Boolean = {
    val hashtag = hashTagExp.findAllIn(StringUtils.stripAccents(text).toLowerCase).toSet
    val mention = mentionExp.findAllIn(StringUtils.stripAccents(text).toLowerCase).toSet

    val hotWords = hashtag ++ mention

    hotWords.foreach(hw => {

      if (polWords.contains(hw)){
        return true
      }
    })
    return false
  }

  /*
    private def findTotalCandidates(text: String): Integer = {
      var candidateFoundTotal = 0

      // find if only one candidate is present in the tweet
      candidates.foreach(c => {
        if (text.toLowerCase.contains(c)) {
          candidateFoundTotal += 1
        }
      })

      return candidateFoundTotal
    }
   */

  def transform(inputFile: String): Unit = {
    val filename = "./input/" + inputFile

    // Check if input file exists
    if (!(new File(filename).exists())) {
      throw new Exception("Input file does not exist: " + filename);
    }

    val linesLength = Source.fromFile(filename).getLines().length
    var lineNumber = 0
    var lineError = 0
    var lineIgnored = 0

    // Pour les statistiques des tweets politiques ou non
    var tweet_pol = 0
    var tweet_non_pol = 0

    println("######")
    println("Extracting " + linesLength + " tweets from " + filename)
    println("######")

    var currentLine: String = ""

    var dateStat : Date = null

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


          // Lève une exception dans le catch si la ligne n'est pas un tweet
          var test = json.getString("created_at")


          // Préparation du format de date
          val s: String = json.getString("created_at").toString
          val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
          val date: Date = simpleDateFormat.parse(s);
          dateStat = date

          // Test si c'est un tweet politique ou non
          if (isPolitique(json.getString("text"))){

            // Incrémente les tweets politiques pour une date
            if(datePol.contains(date)){
              datePol(date) = datePol(date)+1
            } else {
              datePol.put(date, 1)
            }

          } else {

            // Incrémente les tweets non politiques pour une date
            if(dateNonPol.contains(date)){
              dateNonPol(date) = dateNonPol(date)+1
            } else {
              dateNonPol.put(date, 1)
            }

            // On sort de la boucle géante
            break
          }

          // Ajouts des attributs essentiels à l'analyse
          tweet("created_at") = json.getString("created_at")
          tweet("id_str") = json.getString("id_str")
          tweet("candidat") = getCandidate(json.getString("text").toString())
          tweet("sentiment") = TweetFrSent.getSentiment(json.getString("text").toString())

          // Ajouter tous les autres champs dont on pense avoir besoin
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



          // Append in correct file according to the tweet's date
          var write = Json(DefaultFormats).write(tweet).toString
          val outputFile = "./output/" + new SimpleDateFormat("yyyy-MM-dd").format(date) + ".json"

          // Append to new line only if the file contains at least 1 tweet
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
    println(lineError + " " + "tweets have not been extracted (" + ((lineError.toFloat/linesLength.toFloat) * 100) + "% of failure).")
    println(lineIgnored + " " + "have been ignored.")

    // Ecriture d'un Json pour les statistiques des tweet (Pol ou non Pol) par jour
    val outputStatFile = "./output/" + new SimpleDateFormat("yyyy-MM-dd").format(dateStat) + ".json"
    val stats = collection.mutable.Map[String, Any]()
    val fwStat = new FileWriter(outputStatFile, true)
    for((k,v) <- dateNonPol){
      stats("Date") = k
      stats("tweetTotal") = v + datePol.get(k).asInstanceOf[Int]
      stats("tweetPol") = datePol.get(k).asInstanceOf[Int]
      fwStat.write( Json(DefaultFormats).write(stats).toString + "\n")
    }
    fwStat.close()

  }
}
