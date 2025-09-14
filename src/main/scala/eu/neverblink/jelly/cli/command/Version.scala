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

    printLine(f"""
         |jelly-cli   ${BuildInfo.version}
         |-------------------------------------------------------------
         |Jelly-JVM   $jellyV
         |Apache Jena $jenaV
         |JVM         ${System.getProperty("java.vm.name")} ${System.getProperty("java.vm.version")}
         |-------------------------------------------------------------
         |""".stripMargin.trim)
    // Print feature support info
    printReflectionSupport()
    printLargeXmlParsingSupport()
    // Print copyright info
    val buildYear = new SimpleDateFormat("yyyy").format(Date(BuildInfo.buildTime))
    printLine(f"""
         |Copyright (C) $buildYear NeverBlink and contributors.
         |Licensed under the Apache License, Version 2.0.
         |For details, see https://www.apache.org/licenses/LICENSE-2.0
         |This software comes with no warranties and is provided 'as-is'.
         |Documentation and author list: https://github.com/Jelly-RDF/cli
         """.stripMargin)

  private def printReflectionSupport(): Unit =
    val reflectionSupported = JenaSystemOptions.disableTermValidation()
    reflectionSupported match {
      case Failure(ex) =>
        printLine("[ ] JVM reflection: not supported. Parsing will be slower.")
        if getOptions.common.debug then
          printLine("    The exception was:")
          ex.printStackTrace(out)
        else printLine("    Run with --debug for details.")
      case Success(_) => printLine("[X] JVM reflection: supported. Parsing optimizations enabled.")
    }

  private def printLargeXmlParsingSupport(): Unit =
    // See: https://github.com/Jelly-RDF/cli/issues/220
    val maxGeneralEntitySizeLimit = System.getProperty("jdk.xml.maxGeneralEntitySizeLimit")
    val totalEntitySizeLimit = System.getProperty("jdk.xml.totalEntitySizeLimit")
    val ok =
      if Runtime.version().feature() <= 23 then
        // JDK 23 and earlier did not have the new limits, so large XML files are always supported.
        true
      // 50M was the default totalEntitySizeLimit in JDK 23 and earlier.
      // maxGeneralEntitySizeLimit was not defined (0) in JDK 23 and earlier.
      else if maxGeneralEntitySizeLimit != null && maxGeneralEntitySizeLimit.toLong <= 0 &&
        totalEntitySizeLimit != null && (totalEntitySizeLimit.toLong <= 0 || totalEntitySizeLimit.toLong >= 50_000_000)
      then true
      else false

    if ok
    then printLine("[X] Large RDF/XML file parsing: supported.")
    else
      printLine("[ ] Large RDF/XML file parsing: not supported.")
      if getOptions.common.debug then
        printLine(
          f"    jdk.xml.maxGeneralEntitySizeLimit = $maxGeneralEntitySizeLimit",
        )
        printLine(f"    jdk.xml.totalEntitySizeLimit      = $totalEntitySizeLimit")
        printLine("    To enable large XML parsing, set both properties to 0 (no limit).")
      else printLine("    Run with --debug for details.")
