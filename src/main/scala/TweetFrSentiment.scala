import scala.io.Source

object TweetFrSentiment {

  private val hashTagExp = """\B#\w*[a-zA-Z]+\w*""".r
  private val mentionExp = """\B@\w*[a-zA-Z]+\w*""".r
  private val wordExp = """[a-zA-Z]+""".r
  private val dic = new scala.collection.mutable.HashMap[String, Int]()
  private val sentiment = new scala.collection.mutable.HashMap[Int, Double]()
  private val normalization = new scala.collection.mutable.HashMap[String, Double]()

  def main(args: Array[String]) {
    for (line <- Source.fromFile("input/FEEL-1.csv").getLines()) {
      if (!line.startsWith("id") ) {
        val sp = line.split(";")
        var score: Double = 1
        val eval = sp(3)
        dic.put(sp(2),sp(1).toInt)
        if (sp(3).equals("negative")){score = (score * -1) }
        sentiment.put(sp(1).toInt,score)
        println(sp(2) + " : id "+ sp(1).toInt  + "| score : " + score)
      }
    }
  }


  /*

    if (!line.startsWith("id") && !line.startsWith("\t")) {
      val sp = line.split(";")
      val score = 1
        if (sp(3).equals("negative")){val score = (score * -1) }
      val wordToCheck = sp(2)
      val wordId = sp(1)
      if (aggregateScore != 0d) {
        for (synTerm <- sp(4).split(" ")) {
          val wordAndRank = synTerm.split("#")
          val word = wordAndRank(0)
          val rank = wordAndRank(1).toDouble
          sentiments.put(word, sentiments.get(word).getOrElse(0d) + (aggregateScore / rank))
          normalization.put(word, normalization.get(word).getOrElse(0d) + (1d / rank))
        }
      }
    }
  }

  for (word <- sentiments.keys) {
    val normalizedScore = sentiments.get(word).getOrElse(0d) / normalization.get(word).getOrElse(1d)
    sentiments.put(word, normalizedScore)
  }
  normalization.clear()

  def getHashTags(tweet: String): Set[String] = {
    hashTagExp.findAllIn(tweet).toSet
  }

  def getMentions(tweet: String): Set[String] = {
    mentionExp.findAllIn(tweet).toSet
  }

  def getWords(tweet: String): Set[String] = {
    wordExp.findAllIn(tweet).toSet
  }

  def getSentiment(tweet: String): Double = {
    val words = getWords(tweet)
    if (words.isEmpty) {
      return 0d
    } else {
      return words.map(word => sentiments.get(word.toLowerCase()).getOrElse(0d)).reduce(_ + _)
    }
  }
  */

}