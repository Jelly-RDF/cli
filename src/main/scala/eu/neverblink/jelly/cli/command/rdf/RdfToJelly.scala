package eu.neverblink.jelly.cli.command.rdf

import caseapp.*
import com.google.protobuf.TextFormat
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.*
import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat.*
import eu.neverblink.jelly.cli.util.jena.riot.JellyStreamWriterGraphs
import eu.neverblink.jelly.convert.jena.JenaConverterFactory
import eu.neverblink.jelly.convert.jena.riot.{JellyFormatVariant, JellyLanguage, JellyStreamWriter}
import eu.neverblink.jelly.core.{JellyOptions, RdfProtoDeserializationError}
import eu.neverblink.jelly.core.proto.google.v1 as google
import eu.neverblink.jelly.core.proto.v1.{LogicalStreamType, PhysicalStreamType, RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.riot.lang.LabelToNode
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{Lang, RDFParser, RIOT}

import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader, OutputStream}
import scala.util.Using

object RdfToJellyPrint extends RdfCommandPrintUtil[RdfFormat.Readable]:
  override val defaultFormat: RdfFormat = RdfFormat.NQuads

@HelpMessage(
  "Translates an RDF file to a Jelly-RDF stream. \n" +
    "If no input file is specified, the input is read from stdin.\n" +
    "If no output file is specified, the output is written to stdout.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n" +
    "Otherwise, the program will exit with code 0.\n" +
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
      "Jelly file to load and copy serialization options from."
    )
    optionsFrom: Option[String] = None,
    @HelpMessage(
      "Target number of rows per frame – the writer may slightly exceed that. Default: 256",
    )
    rowsPerFrame: Int = 256,
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
) extends HasJellyCommandOptions

object RdfToJelly extends RdfSerDesCommand[RdfToJellyOptions, RdfFormat.Readable]:

  override def names: List[List[String]] = List(
    List("rdf", "to-jelly"),
  )

  lazy val printUtil: RdfCommandPrintUtil[RdfFormat.Readable] = RdfToJellyPrint

  val defaultAction: (InputStream, OutputStream) => Unit =
    langToJelly(RdfFormat.NQuads.jenaLang, _, _)

  def loadOptionsFromFile(filename: String): RdfStreamOptions =
    val inputStream = new FileInputStream(filename)
    val frame = Using(inputStream) {content =>
      RdfStreamFrame.parseDelimitedFrom(content)
    }
    frame.get.getRows.iterator().next().getOptions

  override def doRun(options: RdfToJellyOptions, remainingArgs: RemainingArgs): Unit =
    // Infer before touching options
    options.optionsFrom.map(loadOptionsFromFile).foreach(options.jellySerializationOptions.setOptions)
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
    )
    if !isQuietMode then checkAndWarnTypeCombination()

  override def matchFormatToAction(
      format: RdfFormat.Readable,
  ): Option[(InputStream, OutputStream) => Unit] = format match {
    case f: RdfFormat.Jena.Readable => Some(langToJelly(f.jenaLang, _, _))
    case f: RdfFormat.JellyText.type => Some(jellyTextToJelly)
  }

  /** This method reads the file, rewrites it to Jelly and writes it to some output stream
    * @param jenaLang
    *   Language that should be converted to Jelly
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    */
  private def langToJelly(
      jenaLang: Lang,
      inputStream: InputStream,
      outputStream: OutputStream,
  ): Unit =
    val jellyOpt = getOptions.jellySerializationOptions.asRdfStreamOptions
    // Configure the writer
    val jellyWriter =
      if jellyOpt.getPhysicalType == PhysicalStreamType.GRAPHS then
        // GRAPHS
        JellyStreamWriterGraphs(
          JellyFormatVariant
            .builder()
            .options(
              jellyOpt.clone.setLogicalType(
                if jellyOpt.getLogicalType == LogicalStreamType.UNSPECIFIED then
                  LogicalStreamType.FLAT_QUADS
                else jellyOpt.getLogicalType,
              ),
            )
            .frameSize(getOptions.rowsPerFrame)
            .enableNamespaceDeclarations(getOptions.enableNamespaceDeclarations)
            .isDelimited(getOptions.delimited)
            .build(),
          out = outputStream,
        )
      else
        // TRIPLES or QUADS
        if jellyOpt.getPhysicalType == PhysicalStreamType.UNSPECIFIED then
          if !isQuietMode && isLogicalGrouped(jellyOpt) then
            printLine(
              "WARNING: Logical type setting ignored because physical type is not set. " +
                "Set the physical type to properly pass on the logical type." +
                "Use --quiet to silence this warning.",
              true,
            )
          val writerContext = RIOT.getContext.copy()
            .set(
              JellyLanguage.SYMBOL_STREAM_OPTIONS,
              jellyOpt,
            )
            .set(JellyLanguage.SYMBOL_FRAME_SIZE, getOptions.rowsPerFrame)
            .set(
              JellyLanguage.SYMBOL_ENABLE_NAMESPACE_DECLARATIONS,
              getOptions.enableNamespaceDeclarations,
            ).set(JellyLanguage.SYMBOL_DELIMITED_OUTPUT, getOptions.delimited)
          StreamRDFWriter.getWriterStream(
            outputStream,
            JellyLanguage.JELLY,
            writerContext,
          )
        else
          // If the physical type is specified, we can just construct the writer
          val variant = JellyFormatVariant
            .builder()
            .options(jellyOpt)
            .frameSize(getOptions.rowsPerFrame)
            .enableNamespaceDeclarations(getOptions.enableNamespaceDeclarations)
            .isDelimited(getOptions.delimited)
            .build()
          JellyStreamWriter(JenaConverterFactory.getInstance(), variant, outputStream)

    RDFParser.source(inputStream)
      .lang(jenaLang)
      .labelToNode(LabelToNode.createUseLabelAsGiven())
      .parse(jellyWriter)
    jellyWriter.finish()

  /** Convert Jelly text to Jelly binary.
    * @param inputStream
    *   Jelly text input stream
    * @param outputStream
    *   Jelly binary output stream
    */
  private def jellyTextToJelly(inputStream: InputStream, outputStream: OutputStream): Unit =
    if !isQuietMode then
      printLine(
        "WARNING: The Jelly text format is not stable and may change in incompatible " +
          "ways in the future.\nIt's only intended for testing and development.\n" +
          "NEVER use it in production.\nUse --quiet to silence this warning.",
        true,
      )
    Using.resource(InputStreamReader(inputStream)) { r1 =>
      Using.resource(BufferedReader(r1)) { reader =>
        jellyTextStreamAsFrames(reader)
          .map(txt => TextFormat.parse(txt, classOf[google.RdfStreamFrame]))
          .foreach(frame => {
            if getOptions.delimited then frame.writeDelimitedTo(outputStream)
            else frame.writeTo(outputStream)
          })
      }
    }

  private def checkAndWarnTypeCombination(): Unit = {

    val rdfStreamOptions = getOptions.jellySerializationOptions.asRdfStreamOptions
    val physicalType = rdfStreamOptions.getPhysicalType
    val logicalType = rdfStreamOptions.getLogicalType

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
    if getOptions.delimited then rows.grouped(getOptions.rowsPerFrame).map(_.mkString("\n"))
    else Iterator(rows.mkString("\n"))
