ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

// spark library dependencies
libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.3.0",
  "org.apache.spark" %% "spark-core" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-sql"  % "3.0.0-preview2"
)

lazy val root = (project in file("."))
  .settings(
    name := "dataloader"
  )
