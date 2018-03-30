import sbtassembly.AssemblyKeys._

// sbt-assembly
//assemblySettings

//assemblyJarName in assembly := "ScalaProject.jar"

name := "ScalaJoinMethods"

version := "0.1"

scalaVersion := "2.10.6"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.10
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.2.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.10
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-yarn-common
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-common" % "2.8.0"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-yarn-client
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client" % "2.8.0"

