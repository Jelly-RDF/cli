package eu.neverblink.jelly.cli.command

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.util.jena.JenaSystemOptions

import scala.util.{Failure, Success}

@HelpMessage(
  "Prints the version of the jelly-cli utility and the Jelly-JVM library.",
)
case class VersionOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
) extends HasJellyCommandOptions

object Version extends JellyCommand[VersionOptions]:
  override def names: List[List[String]] = List(
    List("version"),
    List("v"),
    List("--version"),
  )

  override def doRun(options: VersionOptions, remainingArgs: RemainingArgs): Unit =
    val jenaV = BuildInfo.libraryDependencies
      .find(_.startsWith("org.apache.jena:jena-core:")).get.split(":")(2)
    val jellyV = BuildInfo.libraryDependencies
      .find(_.startsWith("eu.neverblink.jelly:jelly-jena:")).get.split(":")(2)
    val reflectionSupported = JenaSystemOptions.disableTermValidation()
    printLine(f"""
         |jelly-cli   ${BuildInfo.version}
         |----------------------------------------------
         |Jelly-JVM   $jellyV
         |Apache Jena $jenaV
         |JVM         ${System.getProperty("java.vm.name")} ${System.getProperty("java.vm.version")}
         |----------------------------------------------
         |""".stripMargin.trim)
    reflectionSupported match {
      case Failure(ex) =>
        printLine("[ ] JVM reflection: not supported. Parsing will be slower.")
        if getOptions.common.debug then
          printLine("    The exception was:")
          ex.printStackTrace(out)
        else printLine("    Run with --debug for details.")
      case Success(_) => printLine("[X] JVM reflection: supported. Parsing optimizations enabled.")
    }
