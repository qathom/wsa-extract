import scala.io.Source
import org.apache.commons.lang3.StringUtils

class TweetFrSentiment {

  private val hashTagExp = """\B#\w*[a-zA-Z]+\w*""".r
  private val mentionExp = """\B@\w*[a-zA-Z]+\w*""".r
  private val wordExp = """[a-zA-Z]+""".r
  private val sentiments = new scala.collection.mutable.HashMap[String, Int]()

  for (line <- Source.fromFile("input/FEEL-1.csv").getLines()) {
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

  def getSentiment(tweet :String ): Double = {
    var words = wordExp.findAllIn(StringUtils.stripAccents(tweet).toLowerCase).toSet
    //for (word <- words) println("The word is : " + word + "\nScore : " + sentiments.get(word).getOrElse(0).asInstanceOf[Int])
    val r = words.map(word => (word, sentiments.get(word).getOrElse(3).asInstanceOf[Int])).filter( (t) => t._2 != 3)
    var size = r.size
    if (size == 0) size = 1
    val sum = r.foldLeft(0.0)(_+_._2)
    val mean: Double = sum / size
    //println("\nThe sum is : " + sum)
    //println("The mean is : " +mean)
    return mean
  }
}