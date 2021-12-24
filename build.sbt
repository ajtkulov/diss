name := "dis"

version := "1.0.0-SNAPSHOT"



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