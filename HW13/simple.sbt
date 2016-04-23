name := "PageRank Project"

version := "1.0"

scalaVersion := "2.10.5"


libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
"org.apache.spark" %% "spark-graphx" % "1.6.1" % "provided"
)

// Reference
// http://www.scala-sbt.org/0.13.1/docs/Getting-Started/Library-Dependencies.html
