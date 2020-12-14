import sbt.Keys.libraryDependencies

name := "spark-learning"

version := "0.1"

ThisBuild / scalaVersion := "2.11.11"


lazy val root = (project in file("."))
  .aggregate(simpleApp)
  .aggregate(kmerApp)

lazy val simpleApp = (project in file("simple-app"))
  .settings(
    // subproject settings
    name := "Spark Simple App",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.11" % "2.2.0"
    )
  )

lazy val kmerApp = (project in file("kmer-basic"))
  .settings(
    name := "Kmer Basic",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.11" % "2.2.0",
      "org.slf4j" % "slf4j-simple" % "1.7.30",
      "com.typesafe" % "config" % "1.4.0"
    )
  )