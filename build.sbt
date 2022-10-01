name := "dis"

version := "1.0.0-SNAPSHOT"

run / javaOptions += "-Xmx16G"

javaOptions in(Test, run) += "-Xmx16G"


resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "com.github.melrief" %% "purecsv" % "0.1.1"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.8.0"

libraryDependencies += "org.jsoup" % "jsoup" % "1.13.1"

libraryDependencies += "com.lihaoyi" %% "requests" % "0.7.0"
libraryDependencies += "com.lihaoyi" %% "ujson" % "0.7.1"

val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.17.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.0",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.0",
)

libraryDependencies += "com.clearspring.analytics" % "stream" % "2.9.8"

// https://mvnrepository.com/artifact/com.typesafe.play/play-json
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.10.0-RC6"

// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.10.14"
