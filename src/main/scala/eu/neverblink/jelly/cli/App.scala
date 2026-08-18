package eu.neverblink.jelly.cli

import caseapp.*
import eu.neverblink.jelly.cli.command.*
import eu.neverblink.jelly.cli.command.rdf.*
import eu.neverblink.jelly.cli.command.sparql.*
import eu.neverblink.jelly.cli.util.jena.riot.CliRiot
import eu.neverblink.jelly.convert.jena.sparql.JellySparqlLanguage
import org.apache.jena.sys.JenaSystem

/** Main entrypoint.
  */
object App extends CommandsEntryPoint:

  // Initialize Jena now to avoid race conditions later
  JenaSystem.init()
  // Initialize the CLI Riot parsers
  CliRiot.initialize()
  // JenaSystem.init() already does this via the subsystem lifecycle, but that relies on service
  // discovery, which we'd rather not depend on in native-image builds. The call is idempotent.
  JellySparqlLanguage.register()

  override def enableCompletionsCommand: Boolean = true

  override def enableCompleteCommand: Boolean = true

  override def progName: String = "jelly-cli"

  override def commands: Seq[Command[?]] = Seq(
    Version,
    RdfFromJelly,
    RdfToJelly,
    RdfTranscode,
    RdfInspect,
    RdfValidate,
    SparqlFromJelly,
    SparqlToJelly,
  )
