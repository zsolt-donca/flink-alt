
lazy val buildSettings = Seq(
  organization := "com.github",
  name := "flink-alt",
  version := "0.17",
  scalaVersion := "2.12.8"
)

val versions = new {
  val flink = "1.7.1"
  val cats = "1.0.1"
  val circe = "0.11.1"
  val shapeless = "2.3.3"
  val scalatest = "3.0.4"
  val scalacheck = "1.13.5"
  val checkless = "1.1.8"
  val slf4j = "1.7.25"
  val paradise = "2.1.0"
}

lazy val compileDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % versions.flink % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % versions.flink % "provided",
  "com.chuusai" %% "shapeless" % versions.shapeless,
  "org.typelevel" %% "cats-core" % versions.cats,
  "net.sf.trove4j" % "trove4j" % "3.0.3",

  "io.circe" %% "circe-core" % versions.circe,
  "io.circe" %% "circe-generic" % versions.circe,
  "io.circe" %% "circe-parser" % versions.circe,
  "io.circe" %% "circe-generic-extras" % versions.circe,
)

lazy val testDependencies = Seq(
  "org.typelevel" %% "cats-laws" % versions.cats,
  "org.scalatest" %% "scalatest" % versions.scalatest,
  "org.scalacheck" %% "scalacheck" % versions.scalacheck,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % versions.checkless,
  "org.slf4j" % "slf4j-nop" % versions.slf4j
).map(_ % "test")

lazy val commonSettings = Seq(
  scalacOptions := Seq(
    "-encoding", "UTF-8",
    //    "-deprecation",
    "-feature",
    "-unchecked",
    "-language:higherKinds",
    "-language:existentials",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-Ywarn-dead-code",
    "-Ywarn-unused",
    "-Ywarn-unused-import",
    "-Ypartial-unification",
    //    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint"
  ),
  libraryDependencies ++= compileDependencies ++ testDependencies,
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full),
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.7")
)

lazy val root = Project("flink-alt", file("."))
  .settings(Defaults.coreDefaultSettings: _*)
  .settings(buildSettings: _*)
  .settings(commonSettings: _*)
