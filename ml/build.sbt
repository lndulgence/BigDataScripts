import Dependencies._

ThisBuild / scalaVersion     := "2.13.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"
ThisBuild / fork in run := true
fork in (Compile, run) := true // ok

lazy val root = (project in file("."))
  .settings(
    name := "example",
    //libraryDependencies += "scalaTest" % "Test",
     name := "ml",
    fork in (Compile, run) := true, 
    javaOptions += "-Xmx3G",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.0",
    libraryDependencies += "org.apache.spark" %% "spark-yarn" % "3.2.2" % "provided"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
