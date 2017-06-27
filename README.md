# scala
scala on spark

build.sbt 配置

name := "hello"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq( "org.apache.spark" %% "spark-core" % "2.1.1", "org.apache.spark" %% "spark-sql" % "2.1.1", "org.apache.spark" %% "spark-streaming" % "2.1.1", "log4j" % "log4j" % "1.2.14" )

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.6"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.6"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.6"

