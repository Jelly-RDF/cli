package eu.neverblink.jelly.cli.command.sparql

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.sparql.util.*
import eu.neverblink.jelly.cli.util.jena.RdfSparqlConverter
import eu.neverblink.jelly.convert.jena.sparql.JellySparqlLanguage
import eu.neverblink.jelly.core.RdfProtoDeserializationError
import eu.neverblink.jelly.core.proto.v1.sparql.SparqlResultsOptions
import eu.neverblink.jelly.core.sparql.{JellySparqlConstants, JellySparqlOptions}
import org.apache.jena.sparql.util.Context

import java.io.{InputStream, OutputStream}

object SparqlToJellyPrint:
  val validFormats: List[SparqlFormat] = SparqlFormat.readable
  val defaultFormat: SparqlFormat = SparqlFormat.Json
  lazy val helpMsg: String = SparqlFormat.helpMsg(validFormats, defaultFormat)

@HelpMessage(
  "Translates SPARQL query results to a Jelly-SPARQL stream. \n" +
    "If no input file is specified, the input is read from stdin.\n" +
    "If no output file is specified, the output is written to stdout.\n" +
    "Both SELECT results (bindings) and ASK results (a boolean) are supported.\n" +
    "The input can also be in the jelly-sparql-text format, which is written by sparql from-jelly.\n" +
    "With the jelly-rdf input format, a Jelly-RDF stream is turned into a result set with one " +
    "solution per statement: ?s ?p ?o for TRIPLES streams, and ?s ?p ?o ?g for QUADS and GRAPHS " +
    "streams, where ?g is unbound for the default graph. Namespace declarations are dropped. " +
    "Unless --opt.rdf-version or --options-from is given, the RDF version of the output is taken " +
    "from the input's options: 1.2 if it may contain triple terms, 1.1 otherwise.\n" +
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
    @Recurse
    jellySerializationOptions: SparqlJellySerializationOptions = SparqlJellySerializationOptions(),
    @HelpMessage(
      "Jelly-SPARQL file to copy serialization options from. Options can be overridden with " +
        "command line --opt.* options. Default: (unset)",
    )
    optionsFrom: Option[String] = None,
    @HelpMessage(
      "Maximum number of values (cells) in one frame. A frame may end earlier, when its lookup " +
        "tables fill up. Default: " + JellySparqlConstants.DEFAULT_MAX_VALUES_PER_FRAME,
    )
    valuesPerFrame: Int = JellySparqlConstants.DEFAULT_MAX_VALUES_PER_FRAME,
    @HelpMessage(
      "Whether the output should be delimited. Setting it to false will force the output to be " +
        "a single frame – make sure you know what you are doing. Default: true",
    )
    delimited: Boolean = true,
) extends HasJellyCommandOptions

object SparqlToJelly extends SparqlSerDesCommand[SparqlToJellyOptions]:

  override def names: List[List[String]] = List(
    List("sparql", "to-jelly"),
  )

  override val validFormats: List[SparqlFormat] = SparqlToJellyPrint.validFormats

  override val defaultFormat: SparqlFormat = SparqlToJellyPrint.defaultFormat

  // Set in doRun, before the conversion starts
  private var baseOptions: Option[SparqlResultsOptions] = None
  private var streamOptions: SparqlResultsOptions = JellySparqlOptions.BIG

  override protected def jenaContext: Context =
    super.jenaContext
      .set(JellySparqlLanguage.SYMBOL_STREAM_OPTIONS, streamOptions)
      .set(JellySparqlLanguage.SYMBOL_MAX_VALUES_PER_FRAME, getOptions.valuesPerFrame)
      .set(JellySparqlLanguage.SYMBOL_DELIMITED_OUTPUT, getOptions.delimited)

  override protected def delimitedOutput: Boolean = getOptions.delimited

  override def doRun(options: SparqlToJellyOptions, remainingArgs: RemainingArgs): Unit =
    val inputFile = remainingArgs.remaining.headOption
    val inputFormat = resolveFormat(options.inputFormat, inputFile)
    if options.valuesPerFrame < 1 then
      throw InvalidArgument(
        "--values-per-frame",
        options.valuesPerFrame.toString,
        Some("Must be at least 1"),
      )
    baseOptions = options.optionsFrom.map(SparqlJellyUtil.loadOptionsFromFile)
    streamOptions = options.jellySerializationOptions.toSparqlResultsOptions(baseOptions)
    if !isQuietMode then checkAndWarnOptions(inputFormat)
    val (inputStream, outputStream) = getIoStreamsFromOptions(inputFile, options.outputFile)
    convert(inputFormat, SparqlFormat.JellySparql, inputStream, outputStream)

  override protected def jellyRdfToSparql(
      inputStream: InputStream,
      outputStream: OutputStream,
  ): Unit =
    RdfSparqlConverter.rdfToSparql(
      inputStream,
      outputStream,
      inputOptions =>
        getOptions.jellySerializationOptions.toSparqlResultsOptions(
          Some(baseOptions.getOrElse(RdfSparqlConverter.defaultSparqlOptions(inputOptions))),
        ),
      getOptions.valuesPerFrame,
      getOptions.delimited,
    )

  private def checkAndWarnOptions(inputFormat: SparqlFormat): Unit =
    if inputFormat == SparqlFormat.JellySparqlText then
      if getOptions.jellySerializationOptions.isAnySet || getOptions.optionsFrom.isDefined then
        printLine(
          "WARNING: Stream options are ignored for jelly-sparql-text input, because the frames " +
            "are copied as they are. Use --quiet to silence this warning.",
          true,
        )
    else
      try
        JellySparqlOptions.checkCompatibility(
          streamOptions,
          JellySparqlOptions.DEFAULT_SUPPORTED_OPTIONS,
        )
      catch
        case e: RdfProtoDeserializationError =>
          printLine(
            "WARNING: Readers with default settings will not accept these stream options: " +
              s"${e.getMessage} Use --quiet to silence this warning.",
            true,
          )
