package eu.neverblink.jelly.cli

import caseapp.*
import eu.neverblink.jelly.cli.command.*

/** Main entrypoint.
  */
object App extends CommandsEntryPoint:
  override def progName: String = "jelly-cli"

  override def commands: Seq[Command[?]] = Seq(
    FoolAround,
    Version,
  )
