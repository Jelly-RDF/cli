package eu.neverblink.jelly.cli

import caseapp.*
import eu.neverblink.jelly.cli.command.*
import eu.neverblink.jelly.cli.command.rdf.*

/** Main entrypoint.
  */
object App extends CommandsEntryPoint:
  override def progName: String = "jelly-cli"

  override def commands: Seq[Command[?]] = Seq(
    FoolAround,
    Version,
    RdfFromJelly,
  )
