package eu.neverblink.jelly.cli.command.sparql

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.sparql.util.SparqlFormat

object SparqlFromJellyPrint:
  val validFormats: List[SparqlFormat] = SparqlFormat.writeable
  val defaultFormat: SparqlFormat = SparqlFormat.Json
  lazy val helpMsg: String = SparqlFormat.helpMsg(validFormats, defaultFormat)

@HelpMessage(
  "Translates a Jelly-SPARQL stream to a different SPARQL result set format. \n" +
    "If no input file is specified, the input is read from stdin.\n" +
    "If no output file is specified, the output is written to stdout.\n" +
    "Both SELECT results (bindings) and ASK results (a boolean) are supported.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n" +
    "Otherwise, the program will exit with code 0.",
)
@ArgsName("<file-to-convert>")
case class SparqlFromJellyOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @HelpMessage(
      "Output file to write the SPARQL results to. If not specified, the output is written to stdout.",
    )
    @ExtraName("to") outputFile: Option[String] = None,
    @HelpMessage(
      "Format the Jelly-SPARQL stream should be translated to. " +
        "If not explicitly specified, but output file supplied, the format is inferred from the file name. " +
        SparqlFromJellyPrint.helpMsg,
    )
    @ExtraName("out-format") outputFormat: Option[String] = None,
) extends HasJellyCommandOptions

object SparqlFromJelly extends SparqlSerDesCommand[SparqlFromJellyOptions]:

  override def names: List[List[String]] = List(
    List("sparql", "from-jelly"),
  )

  override val validFormats: List[SparqlFormat] = SparqlFromJellyPrint.validFormats

  override val defaultFormat: SparqlFormat = SparqlFromJellyPrint.defaultFormat

  override def doRun(options: SparqlFromJellyOptions, remainingArgs: RemainingArgs): Unit =
    val inputFile = remainingArgs.remaining.headOption
    val outputFormat = resolveFormat(options.outputFormat, options.outputFile)
    val (inputStream, outputStream) = getIoStreamsFromOptions(inputFile, options.outputFile)
    convert(SparqlFormat.JellySparql, outputFormat, inputStream, outputStream)
