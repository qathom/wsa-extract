import org.apache.commons.lang3.StringUtils
import scala.io.Source

class TweetFrSentiment {

  /**
    * Regex to get words
    */
  private val wordExp = """[a-zA-Z]+""".r

  /**
    * HashMap containing sentiments
    */
  private val sentiments = new scala.collection.mutable.HashMap[String, Int]()

  for (line <- Source.fromFile("./input/FEEL-1.csv").getLines()) {
    if (!line.startsWith("id")) {
      val sp = line.split(";")
      var score: Int = 1
      val eval = sp(2)
      if (!sp(1).contains(" ")) {
        if (sp(2).equals("negative")) {
          score = (score * -1)
        }
        sentiments.put(StringUtils.stripAccents(sp(1)), score)
      }
    }
  }

  /**
    * Stop words in French are for example "de" "à" "le" "la" "les", etc.
    * It permits to avoid to search the sentiment of such words
    */
  private val stopwords: Seq[String] = Source.fromFile("./input/stopwords.txt").getLines().map(w => StringUtils.stripAccents(w)).toSeq

  def getSentiment(tweet :String ): Double = {
    val words = wordExp.findAllIn(StringUtils.stripAccents(tweet).toLowerCase).toSet.filter(w => !stopwords.contains(w))
    val r = words.map(word => (word, sentiments.get(word).getOrElse(0).asInstanceOf[Int]))
    var size = r.size

    if (size == 0) size = 1
    val sum = r.foldLeft(0.0)(_+_._2)
    val mean: Double = sum / size

    return mean
  }
}