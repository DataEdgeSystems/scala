# scala
scala on spark

build.sbt pizhi

name := "hello"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-streaming" % "2.1.1",
  "log4j" % "log4j" % "1.2.14"
)
