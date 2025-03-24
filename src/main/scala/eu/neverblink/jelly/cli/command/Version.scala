package eu.neverblink.jelly.cli.command

import caseapp.*
import eu.neverblink.jelly.cli.*

case class VersionOptions(
    @Recurse
    common: JellyOptions = JellyOptions(),
) extends HasJellyOptions

object Version extends JellyCommand[VersionOptions]:
  override def names: List[List[String]] = List(
    List("version"),
    List("v"),
  )

  override def doRun(options: VersionOptions, remainingArgs: RemainingArgs): Unit =
    val jenaV = BuildInfo.libraryDependencies
      .find(_.startsWith("org.apache.jena:jena-core:")).get.split(":")(2)
    val jellyV = BuildInfo.libraryDependencies
      .find(_.startsWith("eu.ostrzyciel.jelly:jelly-jena:")).get.split(":")(2)
    printLine(f"""
         |jelly-cli   ${BuildInfo.version}
         |----------------------------------------------
         |Jelly-JVM   $jellyV
         |Apache Jena $jenaV
         |JVM         ${System.getProperty("java.vm.name")} ${System.getProperty("java.vm.version")}
         |""".stripMargin.trim)
