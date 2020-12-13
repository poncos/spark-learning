name := "spark-learning"

version := "0.1"

ThisBuild / scalaVersion := "2.11.11"


lazy val root = (project in file("."))
  .aggregate(simpleApp)
  .aggregate(kmerApp)

lazy val simpleApp = (project in file("simple-app"))
  .settings(
    // subproject settings
  )

lazy val kmerApp = (project in file("kmer-basic"))
  .settings(
    // subproject settings
  )