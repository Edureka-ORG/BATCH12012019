import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.8",
      version      := "1.0-SNAPSHOT"
    )),
    name := "ScalaDemo-BATCH12012019",
    libraryDependencies += scalaTest % Test
  )

  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"