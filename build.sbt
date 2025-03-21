ThisBuild / semanticdbEnabled := true
ThisBuild / scalaVersion := "3.6.4"

resolvers +=
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"

lazy val jenaV = "5.3.0"
lazy val jellyV = "2.8.0"

addCommandAlias("fixAll", "scalafixAll; scalafmtAll")

def isDevBuild: Boolean =
  sys.env.get("DEV_BUILD").exists(s => s != "0" && s != "false")

lazy val root = (project in file("."))
  .enablePlugins(
    BuildInfoPlugin,
    GraalVMNativeImagePlugin,
  )
  .settings(
    name := "jelly-cli",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-simple" % "2.0.17",
      "org.apache.jena" % "jena-core" % jenaV,
      "org.apache.jena" % "jena-arq" % jenaV,
      "eu.ostrzyciel.jelly" %% "jelly-jena" % jellyV,
      "com.github.alexarchambault" %% "case-app" % "2.1.0-M30",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    ),
    scalacOptions ++= Seq(
      "-Wunused:imports",
      "-Werror",
      "-feature",
      "-deprecation",
      "-unchecked",
      "-explain",
    ),
    buildInfoKeys := Seq[BuildInfoKey](
      version,
      scalaVersion,
      libraryDependencies,
    ),
    buildInfoPackage := "eu.neverblink.jelly.cli",

    // GraalVM settings
    Compile / mainClass := Some("eu.neverblink.jelly.cli.App"),
    // Do a fast build if it's a dev build
    graalVMNativeImageOptions := (if (isDevBuild) Seq("-Ob") else Seq("--emit build-report")),
  )
