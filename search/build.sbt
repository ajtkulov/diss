name := "search"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.13.4"


resolvers += Resolver.sonatypeRepo("releases")

//libraryDependencies += "com.lihaoyi" %% "requests" % "0.7.0"
//libraryDependencies += "com.lihaoyi" %% "ujson" % "0.7.1"
libraryDependencies += "com.lihaoyi" %% "cask" % "0.7.3"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.17.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.0",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.0",
)