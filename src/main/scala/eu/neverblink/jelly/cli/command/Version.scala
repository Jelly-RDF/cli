package eu.neverblink.jelly.cli.command

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.util.jena.JenaSystemOptions

import java.text.SimpleDateFormat
import java.util.Date
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
    // Print version info
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
    // Print feature support info
    reflectionSupported match {
      case Failure(ex) =>
        printLine("[ ] JVM reflection: not supported. Parsing will be slower.")
        if getOptions.common.debug then
          printLine("    The exception was:")
          ex.printStackTrace(out)
        else printLine("    Run with --debug for details.")
      case Success(_) => printLine("[X] JVM reflection: supported. Parsing optimizations enabled.")
    }
    // Print copyright info
    val buildYear = new SimpleDateFormat("yyyy").format(Date(BuildInfo.buildTime))
    printLine(f"""
         |Copyright (C) $buildYear NeverBlink and contributors.
         |Licensed under the Apache License, Version 2.0.
         |For details, see https://www.apache.org/licenses/LICENSE-2.0
         |This software comes with no warranties and is provided 'as-is'.
         |Documentation and author list: https://github.com/Jelly-RDF/cli
         """.stripMargin)
