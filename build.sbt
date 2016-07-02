name := "sSimRank"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies  ++= Seq(

  // for hadoop 2.6
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
  // for spark 1.3
  "org.apache.spark" %% "spark-core" % "1.6.2" % "provided",

  "org.apache.spark" %% "spark-graphx" % "1.5.2" % "provided",
  // for scala test
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"

)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "netlib Repository" at "http://repo1.maven.org/maven2/"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
