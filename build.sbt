ThisBuild / semanticdbEnabled := true
lazy val scalaV = "3.7.2"
ThisBuild / scalaVersion := scalaV

resolvers +=
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"

lazy val jenaV = "5.3.0"
lazy val jellyV = "3.5.0"
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
  // For the release build, optimize for speed and make a build report
  if (isDevBuild) Seq("-Ob") else Seq("-O3", "--emit build-report"),
).flatten ++ Seq(
  "--features=eu.neverblink.jelly.cli.graal.ProtobufFeature," +
    "eu.neverblink.jelly.cli.graal.JenaInternalsFeature",
  "-H:ReflectionConfigurationFiles=" + file("graal.json").getAbsolutePath,
  // Needed to skip initializing all charsets.
  // See: https://github.com/Jelly-RDF/cli/issues/154
  "--initialize-at-build-time=org.glassfish.json.UnicodeDetectingInputStream",
  "-H:+TrackPrimitiveValues", // SkipFlow optimization -- will be default in GraalVM 25
  "-H:+UsePredicates", // SkipFlow optimization -- will be default in GraalVM 25
  // Make sure we don't include stuff that should be unreachable in the native image
  "-H:AbortOnMethodReachable=*UUID.randomUUID*",
)

lazy val TestSerial = config("test-serial") extend Test

lazy val root = (project in file("."))
  .enablePlugins(
    BuildInfoPlugin,
    GraalVMNativeImagePlugin,
  )
  .configs(TestSerial)
  .settings(
    name := "jelly-cli",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-simple" % "2.0.17",
      "org.apache.jena" % "jena-core" % jenaV,
      "org.apache.jena" % "jena-arq" % jenaV,
      // Jelly-JVM >= 3.4.1 includes Jena 5.5.x as a dependency, we must exclude it, because
      // we use Jena 5.3.0.
      ("eu.neverblink.jelly" % "jelly-jena" % jellyV).excludeAll(ExclusionRule("org.apache.jena")),
      "eu.neverblink.jelly" % "jelly-core-protos-google" % jellyV,
      "com.github.alexarchambault" %% "case-app" % "2.1.0",
      "org.scalatest" %% "scalatest" % "3.2.19" % "test,test-serial",
      "org.yaml" % "snakeyaml" % "2.5" % Test,
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
      BuildInfoKey.action("buildTime") {
        System.currentTimeMillis
      },
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

    // Serial tests should not run in parallel.
    // They are used for tests that manipulate global state, like system properties.
    inConfig(TestSerial)(Defaults.testSettings),
    TestSerial / parallelExecution := false,

    // GraalVM settings
    Compile / mainClass := Some("eu.neverblink.jelly.cli.App"),
    // Do a fast build if it's a dev build
    // For the release build, optimize for speed and make a build report
    graalVMNativeImageOptions := graalOptions,
  )
