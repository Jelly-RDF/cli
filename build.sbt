ThisBuild / semanticdbEnabled := true
lazy val scalaV = "3.7.2"
ThisBuild / scalaVersion := scalaV

resolvers +=
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"

lazy val jenaV = "5.3.0"
lazy val jellyV = "3.4.1"
lazy val graalvmV = "24.2.2"

addCommandAlias("fixAll", "scalafixAll; scalafmtAll")

lazy val wErrorIfCI = if (sys.env.contains("CI")) Seq("-Werror") else Seq()

def isDevBuild: Boolean =
  sys.env.get("DEV_BUILD").exists(s => s != "0" && s != "false")

lazy val graalOptions = Seq(
  // If running on Scala <3.8 and JDK >=24, we need to allow unsafe memory access.
  // Otherwise, we get annoying warnings on startup.
  // https://github.com/scala/scala3/issues/9013
  // Remove this after moving to Scala 3.8
  if (scalaV.split('.')(1).toInt < 8) Seq("-J--sun-misc-unsafe-memory-access=allow") else Nil,
  // Do a fast build if it's a dev build
  // For the release build, optimize for size and make a build report
  if (isDevBuild) Seq("-Ob") else Seq("-Os", "--emit build-report"),
).flatten ++ Seq(
  "--features=eu.neverblink.jelly.cli.graal.ProtobufFeature",
  "-H:ReflectionConfigurationFiles=" + file("graal.json").getAbsolutePath,
  "-H:+AddAllCharsets", // TODO: only add necessary charsets github.com/Jelly-RDF/cli/issues/154
)

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
      "eu.neverblink.jelly" % "jelly-jena" % jellyV,
      "eu.neverblink.jelly" % "jelly-core-protos-google" % jellyV,
      "com.github.alexarchambault" %% "case-app" % "2.1.0-M30",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.yaml" % "snakeyaml" % "2.4" % Test,
      // For native-image reflection compatibility
      "org.graalvm.sdk" % "graal-sdk" % graalvmV % "provided",
      "org.reflections" % "reflections" % "0.10.2",
    ),
    scalacOptions ++= Seq(
      "-Wunused:imports",
      "-feature",
      "-deprecation",
      "-unchecked",
      "-explain",
    ) ++ wErrorIfCI,
    buildInfoKeys := Seq[BuildInfoKey](
      version,
      scalaVersion,
      libraryDependencies,
    ),
    buildInfoPackage := "eu.neverblink.jelly.cli",
    assembly / assemblyMergeStrategy := {
      case PathList("module-info.class") => MergeStrategy.discard
      // https://jena.apache.org/documentation/notes/jena-repack.html
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("reference.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    },

    // GraalVM settings
    Compile / mainClass := Some("eu.neverblink.jelly.cli.App"),
    // Do a fast build if it's a dev build
    // For the release build, optimize for size and make a build report
    graalVMNativeImageOptions := graalOptions,
  )
