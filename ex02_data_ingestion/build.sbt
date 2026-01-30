ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex02_data_ingestion",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core" % "3.5.5",
      "org.apache.spark"  %% "spark-sql"  % "3.5.5",
      "org.apache.hadoop" %  "hadoop-aws" % "3.3.4",
      "com.amazonaws"     %  "aws-java-sdk-bundle" % "1.12.262",
      // Ajout pour l'exercice 2 (Branche 2 - Postgres)
      "org.postgresql"    %  "postgresql" % "42.7.2"
    )
  )
