name := "wsa-extract-2017"

version := "1.0"

scalaVersion := "2.11.0"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies += "org.codehaus.jettison" % "jettison" % "1.3.7"

libraryDependencies += "co.theasi" %% "plotly" % "0.2.0"
libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.3.1"
