import java.io.FileWriter

import Main.formatDate
import org.codehaus.jettison.json.JSONObject
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

class ChartBuilder {

  def build(): Unit = {
    buildTimeline()
  }

  def buildTimeline(): Unit = {
    //
    // chart 1
    //
    val tstat = new TweetStatistics
    val json = tstat.getData()

    // x-axis represents the dates
    val x:Seq[String] =
      for (i <- 0 to json.length() - 1)
        yield formatDate(json.names().get(i).toString)

    // y1-axis represents not political tweets
    val y1:Seq[Double] =
      for (i <- 0 to json.length() - 1)
        yield new JSONObject(json.get(json.names().get(i).toString).toString).getDouble("notPolitics")

    // y2-axis represents political tweets
    val y2:Seq[Double] =
      for (i <- 0 to json.length() - 1)
        yield new JSONObject(json.get(json.names().get(i).toString).toString).getDouble("politics")

    var script = ""
    script += "var ctx = document.querySelector('#chart').getContext('2d')\n" +
      "var myLineChart = new Chart(ctx, {\n" +
      "type: 'line',\n" +
      "data: {\n" +
      "labels: " + Json(DefaultFormats).write(x).toString + ",\n" +
      "datasets: [{\n" +
      "label: 'Not political',\n" +
      "data: " + Json(DefaultFormats).write(y1).toString + ",\n" +
      "backgroundColor: 'rgba(153,255,51,0.4)'\n" +
      "}, {\n" +
      "label: 'Political',\n" +
      "data: " + Json(DefaultFormats).write(y2).toString + ",\n" +
      "backgroundColor: 'rgba(255,153,0,0.4)'\n" +
      "}]\n" +
      "}\n" +
      "})\n"

    val html = "<html><head>\n\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.5.0/Chart.js\"></script>\n</head>\n\n<body>\n<div style=\"width:600px\"><canvas id=\"chart\" width=\"400\" height=\"400\"></canvas>\n  <script>\n " + script + "\n  </script>\n</div>\n</body></html>"

    val fw = new FileWriter("output/chart1.html", false)
    try {
      fw.write(html)
    } finally {
      fw.close()
    }
  }
}
