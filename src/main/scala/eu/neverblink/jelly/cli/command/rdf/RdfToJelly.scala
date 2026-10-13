package eu.neverblink.jelly.cli.command.rdf

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.*
import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat.*
import eu.neverblink.jelly.cli.util.io.ProtoText
import eu.neverblink.jelly.cli.util.jena.{JenaSystemOptions, RdfSparqlConverter}
import eu.neverblink.jelly.cli.util.jena.riot.{JellyWriterUtil, RiotParserUtil}
import eu.neverblink.jelly.core.{
  JellyOptions,
  RdfProtoDeserializationError,
  RdfProtoSerializationError,
}
import eu.neverblink.jelly.core.proto.v1.*

import java.io.{BufferedReader, InputStream, InputStreamReader, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable.ArrayBuffer
import scala.util.Using

object RdfToJellyPrint extends RdfCommandPrintUtil[RdfFormat.Readable]:
  override val defaultFormat: RdfFormat = RdfFormat.NQuads

@HelpMessage(
  "Translates an RDF file to a Jelly-RDF stream. \n" +
    "If no input file is specified, the input is read from stdin.\n" +
    "If no output file is specified, the output is written to stdout.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n" +
    "Otherwise, the program will exit with code 0.\n" +
    "The input can also be a Jelly-SPARQL result set (the jelly-sparql format) with 3 or 4 " +
    "variables, which are taken by position, not by name: subject, predicate, object, and graph. " +
    "Each solution becomes one statement. With 3 variables, the output is a TRIPLES stream, " +
    "with 4 a QUADS stream, where an unbound graph means the default graph. Each result set of " +
    "a PUNCTUATED stream becomes one RDF Message (stream type messages), unless the stream type " +
    "is set with --opt.stream-type.\n" +
    "Note: this command works in a streaming manner and scales well to large files. ",
)
@ArgsName("<file-to-convert>")
case class RdfToJellyOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @HelpMessage(
      "Output file to write the Jelly to. If not specified, the output is written to stdout.",
    )
    @ExtraName("to") outputFile: Option[String] = None,
    @HelpMessage(
      "RDF format of the data that should be translated to Jelly. " +
        "If not explicitly specified, but input file supplied, the format is inferred from the file name. " +
        RdfToJellyPrint.helpMsg,
    )
    @ExtraName("in-format") inputFormat: Option[String] = None,
    @Recurse
    jellySerializationOptions: RdfJellySerializationOptions = RdfJellySerializationOptions(),
    @HelpMessage(
      "Jelly file to copy serialization options from. Options can be overridden with command line --opt.* options. Default: (unset)",
    )
    optionsFrom: Option[String] = None,
    @HelpMessage(
      "Target number of statements per frame (in Jelly-RDF 1.1: rows, and the writer may " +
        "slightly exceed that). Default: " +
        RdfJellySerializationOptions.defaultRowsPerFrame(rowLayout =
          false,
        ) + " (in Jelly-RDF 1.1: " +
        RdfJellySerializationOptions.defaultRowsPerFrame(rowLayout = true) + ")",
    )
    rowsPerFrame: Option[Int] = None,
    @HelpMessage(
      "Whether to preserve explicit namespace declarations in the output (PREFIX: in Turtle). " +
        "Default: false",
    )
    enableNamespaceDeclarations: Boolean = false,
    @HelpMessage(
      "Whether the output should be delimited. Setting it to false will force the output to be a single " +
        "frame – make sure you know what you are doing. Default: true",
    )
    delimited: Boolean = true,
    @Recurse
    rdfPerformanceOptions: RdfPerformanceOptions = RdfPerformanceOptions(),
) extends HasJellyCommandOptions:
  def frameSize: Int = rowsPerFrame.getOrElse(
    RdfJellySerializationOptions.defaultRowsPerFrame(jellySerializationOptions.isRowLayout),
  )

object RdfToJelly extends RdfSerDesCommand[RdfToJellyOptions, RdfFormat.Readable]:

  override def names: List[List[String]] = List(
    List("rdf", "to-jelly"),
  )

  lazy val printUtil: RdfCommandPrintUtil[RdfFormat.Readable] = RdfToJellyPrint

  val defaultAction: WriteAction =
    langToJelly(RdfFormat.NQuads, _, _, _)

  override def doRun(options: RdfToJellyOptions, remainingArgs: RemainingArgs): Unit =
    if !options.rdfPerformanceOptions.validateTerms.getOrElse(false) then
      JenaSystemOptions.disableTermValidation()
    // Infer before touching options
    options.optionsFrom.map(JellyUtil.loadOptionsFromFile).foreach(
      options.jellySerializationOptions.setOptions,
    )
    options.jellySerializationOptions.inferGeneralized(
      options.inputFormat,
      remainingArgs.remaining.headOption,
    )
    // Touch the options to make sure they are valid
    options.jellySerializationOptions.asRdfStreamOptions
    val (inputStream, outputStream) =
      getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    parseFormatArgs(
      inputStream,
      outputStream,
      options.inputFormat,
      remainingArgs.remaining.headOption,
      options,
    )
    if !isQuietMode then checkAndWarnTypeCombination()

  override def matchFormatToAction(
      format: RdfFormat.Readable,
  ): Option[WriteAction] = format match {
    case f: RdfFormat.Jena.Readable => Some(langToJelly(f, _, _, _))
    case f: RdfFormat.JellyText.type => Some(jellyTextToJelly)
    case RdfFormat.JellySparql => Some(jellySparqlToJelly)
  }

  /** Convert a Jelly-SPARQL result set of ?s ?p ?o (?g) to Jelly-RDF. */
  private def jellySparqlToJelly(
      inputStream: InputStream,
      outputStream: OutputStream,
      opt: RdfToJellyOptions,
  ): Unit =
    RdfSparqlConverter.sparqlToRdf(
      inputStream,
      outputStream,
      getOptions.jellySerializationOptions.asRdfStreamOptions,
      streamTypeSet = getOptions.jellySerializationOptions.`opt.streamType`.isDefined,
      getOptions.frameSize,
      getOptions.delimited,
    )

  /** This method reads the file, rewrites it to Jelly and writes it to some output stream
    * @param format
    *   Language that should be converted to Jelly
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    */
  private def langToJelly(
      format: RdfFormat.Jena,
      inputStream: InputStream,
      outputStream: OutputStream,
      opt: RdfToJellyOptions,
  ): Unit =
    val jellyOpt = getOptions.jellySerializationOptions.asRdfStreamOptions
    if StreamOptionsUtil.isRowLayout(jellyOpt) &&
      jellyOpt.getPhysicalType == PhysicalStreamType.UNSPECIFIED && !isQuietMode &&
      isLogicalGrouped(jellyOpt)
    then
      printLine(
        "WARNING: Logical type setting ignored because physical type is not set. " +
          "Set the physical type to properly pass on the logical type." +
          "Use --quiet to silence this warning.",
        true,
      )
    val jellyWriter = JellyWriterUtil.createWriter(
      jellyOpt,
      getOptions.frameSize,
      getOptions.enableNamespaceDeclarations,
      getOptions.delimited,
      outputStream,
    )

    try
      RiotParserUtil.parse(
        getOptions.rdfPerformanceOptions.resolveIris,
        format,
        inputStream,
        jellyWriter,
      )
      jellyWriter.finish()
    catch
      // Generalized statements and RDF-star can only be written in Jelly-RDF 1.1
      case e: RdfProtoSerializationError if e.getMessage.contains("Jelly-RDF 1.1") =>
        throw JellySerializationError(
          e.getMessage + " Use --opt.jelly-version=1.1 to write Jelly-RDF 1.1.",
        )

  /** Convert Jelly text to Jelly binary.
    * @param inputStream
    *   Jelly text input stream
    * @param outputStream
    *   Jelly binary output stream
    */
  private def jellyTextToJelly(
      inputStream: InputStream,
      outputStream: OutputStream,
      opt: RdfToJellyOptions,
  ): Unit =
    if !isQuietMode then
      printLine(
        "WARNING: The Jelly text format is not stable and may change in incompatible " +
          "ways in the future.\nIt's only intended for testing and development.\n" +
          "NEVER use it in production.\nUse --quiet to silence this warning.",
        true,
      )
    Using.resource(InputStreamReader(inputStream, UTF_8)) { r1 =>
      Using.resource(BufferedReader(r1)) { reader =>
        jellyTextStreamAsFrames(reader)
          .map(txt => ProtoText.parse(RdfStreamFrame.getDescriptor, txt))
          .foreach(frame => {
            if getOptions.delimited then frame.writeDelimitedTo(outputStream)
            else frame.writeTo(outputStream)
          })
      }
    }

  /** Warns about logical types that do not fit the physical type (only Jelly-RDF 1.1). */
  private def checkAndWarnTypeCombination(): Unit = {

    val rdfStreamOptions = getOptions.jellySerializationOptions.asRdfStreamOptions
    val physicalType = rdfStreamOptions.getPhysicalType
    val logicalType = rdfStreamOptions.getLogicalType

    if StreamOptionsUtil.isRowLayout(rdfStreamOptions) then
      try {
        // This check will find physical/logical clashes, since all other fields are checked with <=,
        // so they pass when compared against themselves
        JellyOptions.checkCompatibility(rdfStreamOptions, rdfStreamOptions)
      } catch {
        case _: RdfProtoDeserializationError =>
          printLine(
            s"WARNING: Selected combination of logical/physical stream types ($logicalType/$physicalType) is unsupported. " +
              "Use --quiet to silence this warning.",
            true,
          )
      }
  }

  /** Check if the logical type is defined and grouped.
    * @param jellyOpt
    *   the Jelly options
    * @return
    *   true if the logical type is specified and expects framing
    */
  private def isLogicalGrouped(
      jellyOpt: RdfStreamOptions,
  ): Boolean =
    !(jellyOpt.getLogicalType == LogicalStreamType.FLAT_QUADS || jellyOpt.getLogicalType == LogicalStreamType.FLAT_TRIPLES || jellyOpt.getLogicalType == LogicalStreamType.UNSPECIFIED)

  /** Iterate over a Jelly text stream and return the frames as strings to be parsed.
    * @param reader
    *   the reader to read from
    * @return
    *   an iterator of Jelly text frames
    */
  private def jellyTextStreamAsFrames(reader: BufferedReader): Iterator[String] =
    val buffer = new StringBuilder()
    val rows = Iterator.continually(()).map { _ =>
      reader.readLine() match {
        case null =>
          val s = buffer.toString()
          buffer.clear()
          (Some(s), false)
        case line if line.startsWith("}") =>
          buffer.append(line)
          buffer.append("\n")
          val s = buffer.toString()
          buffer.clear()
          (Some(s), true)
        case line =>
          buffer.append(line)
          buffer.append("\n")
          (None, true)
      }
    }.takeWhile(_._2).collect({ case (Some(row), _) => row })

    // The only options we can respect in this mode are the frame size and the delimited flag
    // The others are ignored, because we are doing a 1:1 conversion
    if !getOptions.delimited then Iterator(rows.mkString("\n"))
    else
      // Rows are grouped into frames of the given size. A Jelly-RDF 1.2 frame ends with its
      // column batch, so the rows (stream options) before it go into the same frame.
      val frameSize = getOptions.rowsPerFrame
        .getOrElse(RdfJellySerializationOptions.defaultRowsPerFrame(rowLayout = true))
      val frame = ArrayBuffer[String]()
      def isColumns(block: String) =
        block.linesIterator.find(!_.startsWith("#")).exists(_.startsWith("columns"))
      rows.flatMap { block =>
        frame += block
        if isColumns(block) || frame.size >= frameSize then
          val text = frame.mkString("\n")
          frame.clear()
          Some(text)
        else None
      } ++ Iterator.single(()).flatMap(_ => Option.when(frame.nonEmpty)(frame.mkString("\n")))
