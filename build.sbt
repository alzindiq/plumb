name := "Plumb"

version := "0.1"

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
  "io.spray" %% "spray-json" % "1.3.1",
  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

val buildSettings = Defaults.defaultSettings ++ Seq(
  javaOptions += "-Xmx8G"
)
