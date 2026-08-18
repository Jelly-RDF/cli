package eu.neverblink.jelly.cli.command.sparql

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.sparql.util.SparqlFormat

object SparqlToJellyPrint:
  val validFormats: List[SparqlFormat] = SparqlFormat.readable
  val defaultFormat: SparqlFormat = SparqlFormat.Json
  lazy val helpMsg: String = SparqlFormat.helpMsg(validFormats, defaultFormat)

@HelpMessage(
  "Translates SPARQL query results to a Jelly-SPARQL stream. \n" +
    "If no input file is specified, the input is read from stdin.\n" +
    "If no output file is specified, the output is written to stdout.\n" +
    "Both SELECT results (bindings) and ASK results (a boolean) are supported.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n" +
    "Otherwise, the program will exit with code 0.",
)
@ArgsName("<file-to-convert>")
case class SparqlToJellyOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @HelpMessage(
      "Output file to write the Jelly-SPARQL to. If not specified, the output is written to stdout.",
    )
    @ExtraName("to") outputFile: Option[String] = None,
    @HelpMessage(
      "Format of the SPARQL results that should be translated to Jelly. " +
        "If not explicitly specified, but input file supplied, the format is inferred from the file name. " +
        SparqlToJellyPrint.helpMsg,
    )
    @ExtraName("in-format") inputFormat: Option[String] = None,
) extends HasJellyCommandOptions

object SparqlToJelly extends SparqlSerDesCommand[SparqlToJellyOptions]:

  override def names: List[List[String]] = List(
    List("sparql", "to-jelly"),
  )

  override val validFormats: List[SparqlFormat] = SparqlToJellyPrint.validFormats

  override val defaultFormat: SparqlFormat = SparqlToJellyPrint.defaultFormat

  override def doRun(options: SparqlToJellyOptions, remainingArgs: RemainingArgs): Unit =
    val inputFile = remainingArgs.remaining.headOption
    val inputFormat = resolveFormat(options.inputFormat, inputFile)
    val (inputStream, outputStream) = getIoStreamsFromOptions(inputFile, options.outputFile)
    convert(inputFormat, SparqlFormat.JellySparql, inputStream, outputStream)
