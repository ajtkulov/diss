name := "gen"

version := "1.0.0-SNAPSHOT"

lazy val commonSettings = Seq(
  organization := "diss",
  scalaVersion := "2.13.6",
  sources in(Compile, doc) := Seq.empty,
  publishArtifact in(Compile, packageDoc) := false,
  publishArtifact in(Compile, packageSrc) := false
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.15"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"
