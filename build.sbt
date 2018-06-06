
lazy val buildSettings = Seq(
  organization := "com.github",
  name := "flink-alt",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.11.12"
)

val versions = new {
  val flink = "1.4.0"
  val cats = "1.0.1"
  val simulacrum = "0.12.0"
  val shapeless = "2.3.3"
  val scalatest = "3.0.4"
  val scalacheck = "1.13.5"
  val checkless = "1.1.8"
  val scala_arm = "2.0"
  val utils = "1.07.00"
  val slf4j = "1.7.25"
  val paradise = "2.1.0"
  val scalaJavaTime = "2.0.0-M13"
}

lazy val compileDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % versions.flink,
  "org.apache.flink" %% "flink-streaming-scala" % versions.flink,
  "com.chuusai" %% "shapeless" % versions.shapeless,
  "com.github.mpilquist" %% "simulacrum" % versions.simulacrum,
  "org.typelevel" %% "cats-core" % versions.cats,
  "io.github.cquiroz" %% "scala-java-time" % versions.scalaJavaTime,
  "net.sf.trove4j" % "trove4j" % "3.0.3"
)

lazy val testDependencies = Seq(
  "org.typelevel" %% "cats-laws" % versions.cats,
  "org.scalatest" %% "scalatest" % versions.scalatest,
  "org.scalacheck" %% "scalacheck" % versions.scalacheck,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % versions.checkless,
  "com.jsuereth" %% "scala-arm" % versions.scala_arm,
  "org.ostermiller" % "utils" % versions.utils,
  "org.slf4j" % "slf4j-nop" % versions.slf4j
).map(_ % "test")

lazy val commonSettings = Seq(
  scalacOptions := Seq(
    "-encoding", "UTF-8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-Ywarn-dead-code",
    "-Ywarn-unused",
    //    "-Ywarn-unused-import",
    "-Ypartial-unification",
    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint"
  ),
  libraryDependencies ++= compileDependencies ++ testDependencies,
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full),
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.6")
)

lazy val root = Project("flink-alt", file("."))
  .settings(Defaults.coreDefaultSettings: _*)
  .settings(buildSettings: _*)
  .settings(commonSettings: _*)
