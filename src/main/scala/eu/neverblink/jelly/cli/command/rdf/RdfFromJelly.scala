package eu.neverblink.jelly.cli.command.rdf

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.*
import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat.*
import eu.neverblink.jelly.cli.util.args.IndexRange
import eu.neverblink.jelly.cli.util.jena.{
  JenaSystemOptions,
  StreamRdfBatchWriter,
  StreamRdfCombiningBatchWriter,
}
import eu.neverblink.jelly.convert.jena.JenaConverterFactory
import eu.neverblink.jelly.core.JellyOptions
import eu.neverblink.jelly.core.RdfHandler.AnyStatementHandler
import eu.neverblink.jelly.core.proto.v1.RdfStreamFrame
import eu.neverblink.jelly.core.proto.google.v1 as google
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.sparql.core.Quad

import java.io.{InputStream, OutputStream}
import scala.jdk.CollectionConverters.*

object RdfFromJellyPrint extends RdfCommandPrintUtil[RdfFormat.Writeable]:
  override val defaultFormat: RdfFormat = RdfFormat.NQuads

@HelpMessage(
  "Translates a Jelly-RDF stream to a different RDF format. \n" +
    "If no input file is specified, the input is read from stdin.\n" +
    "If no output file is specified, the output is written to stdout.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n" +
    "Otherwise, the program will exit with code 0.\n" +
    "Note: this command works in a streaming manner where possible and scales well to\n" +
    "large files. Non-streaming formats (e.g. RDF/XML) by default work on a\n" +
    "frame-by-frame basis, but they can be combined into one dataset with the\n" +
    "--combine option. RDF/XML will only serialize the default model.",
)
@ArgsName("<file-to-convert>")
case class RdfFromJellyOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @HelpMessage(
      "Output file to write the RDF to. If not specified, the output is written to stdout.",
    )
    @ExtraName("to") outputFile: Option[String] = None,
    @HelpMessage(
      "RDF format Jelly should be translated to. " +
        "If not explicitly specified, but output file supplied, the format is inferred from the file name. " + RdfFromJellyPrint.helpMsg,
    )
    @ExtraName("out-format") outputFormat: Option[String] = None,
    @HelpMessage(
      "Frame indices to include in the output. If not specified, all frames are included. " +
        IndexRange.helpText,
    )
    takeFrames: String = "",
    @HelpMessage(
      "Add to combine the results into one dataset, when using a non-streaming output format. " +
        "Ignored otherwise. Take care with input size, as this option will load everything into memory.",
    )
    combine: Boolean = false,
    @HelpMessage(
      "Discard the named graph information, treating the input as triples in the default graph. " +
        "This allows you to convert a Jelly file containing quads to Turtle/N-Triples in a lossy manner. " +
        "This option has no impact on frame boundaries. To merge frames, use the --combine option.",
    )
    mergeGraphs: Boolean = false,
    @Recurse
    rdfPerformanceOptions: RdfPerformanceOptions = RdfPerformanceOptions(),
) extends HasJellyCommandOptions

object RdfFromJelly extends RdfSerDesCommand[RdfFromJellyOptions, RdfFormat.Writeable]:

  override def names: List[List[String]] = List(
    List("rdf", "from-jelly"),
  )

  lazy val printUtil: RdfCommandPrintUtil[RdfFormat.Writeable] = RdfFromJellyPrint

  val defaultAction: WriteAction =
    (in, out, opt) =>
      jellyToLang(
        in,
        StreamRDFWriter.getWriterStream(out, RdfFormat.NQuads.jenaLang),
        RdfFormat.NQuads,
        opt,
      )

  private def takeFrames: IndexRange = IndexRange(getOptions.takeFrames, "--take-frames")

  override def doRun(options: RdfFromJellyOptions, remainingArgs: RemainingArgs): Unit =
    if !options.rdfPerformanceOptions.validateTerms.getOrElse(false) then
      JenaSystemOptions.disableTermValidation()
    // Parse options now to make sure they are valid
    takeFrames
    val (inputStream, outputStream) =
      this.getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    parseFormatArgs(inputStream, outputStream, options.outputFormat, options.outputFile, options)

  override def matchFormatToAction(
      format: RdfFormat.Writeable,
  ): Option[WriteAction] =
    (format, getOptions.combine) match
      case (j: RdfFormat.Jena.StreamWriteable, _) =>
        Some((in, out, opt) =>
          jellyToLang(in, StreamRDFWriter.getWriterStream(out, j.jenaLang), j, opt),
        )
      case (j: RdfFormat.Jena.BatchWriteable, true) =>
        Some((in, out, opt) =>
          StreamRdfCombiningBatchWriter(out, j.jenaLang).runAndOutput(x =>
            jellyToLang(in, x, j, opt),
          ),
        )
      case (j: RdfFormat.Jena.BatchWriteable, false) =>
        Some((in, out, opt) => jellyToLang(in, StreamRdfBatchWriter(out, j.jenaLang), j, opt))
      case (RdfFormat.JellyText, _) => Some(jellyBinaryToText)

  /** This method reads the Jelly file, rewrites it to specified format and writes it to some output
    * stream
    */
  private def jellyToLang(
      inputStream: InputStream,
      writer: StreamRDF,
      format: RdfFormat,
      options: RdfFromJellyOptions,
  ): Unit =
    // Whether the output is active at this moment
    var outputEnabled = false
    val handler = new AnyStatementHandler[Node] {
      override def handleNamespace(prefix: String, namespace: Node): Unit = {
        if outputEnabled then writer.prefix(prefix, namespace.getURI)
      }

      override def handleTriple(subject: Node, predicate: Node, `object`: Node): Unit = {
        if outputEnabled then writer.triple(Triple.create(subject, predicate, `object`))
      }

      override def handleQuad(subject: Node, predicate: Node, `object`: Node, graph: Node): Unit = {
        if outputEnabled then
          if format.supportsQuads then writer.quad(Quad.create(graph, subject, predicate, `object`))
          else if options.mergeGraphs || Quad.isDefaultGraph(graph) then
            writer.triple(Triple.create(subject, predicate, `object`))
          else
            throw new CriticalException(
              f"Encountered a quad in the input ($subject $predicate ${`object`} $graph), " +
                f"but the output format ($format) does not support quads. Either choose a different output format " +
                "or use the --merge-graphs option to merge all named graphs into the default graph.",
            )
      }
    }

    val decoder = JenaConverterFactory.getInstance().anyStatementDecoder(
      // Only pass on the namespaces to the writer if the output is enabled
      handler,
      JellyOptions.DEFAULT_SUPPORTED_OPTIONS,
    )

    val inputFrames = takeFrames.end match
      case Some(end) => JellyUtil.iterateRdfStream(inputStream).take(end)
      case None => JellyUtil.iterateRdfStream(inputStream)
    val startFrom = takeFrames.start.getOrElse(0)
    for (frame, i) <- inputFrames.zipWithIndex do
      // If we are not yet in the output range, still fully parse the frame and update the decoder
      // state. We need this to decode the later frames correctly.
      if i < startFrom then for row <- frame.getRows.asScala do decoder.ingestRow(row)
      else
        // TODO: write frame index as a comment here
        //   https://github.com/Jelly-RDF/cli/issues/4
        outputEnabled = true
        // We are in the output range, so we can start writing the output
        for row <- frame.getRows.asScala do decoder.ingestRow(row)
        writer.finish()

  /** This method reads the Jelly file, rewrites it to Jelly text and writes it to some output
    * stream
    */
  private def jellyBinaryToText(
      inputStream: InputStream,
      outputStream: OutputStream,
      opt: RdfFromJellyOptions,
  ): Unit =
    inline def writeFrameToOutput(f: RdfStreamFrame, frameIndex: Int): Unit =
      // we want to write a comment to the file before each frame
      val comment = f"# Frame $frameIndex\n"
      outputStream.write(comment.getBytes)
      val frame = google.RdfStreamFrame.parseFrom(f.toByteArray).toString
      // the protoString is basically the jelly-txt format already
      outputStream.write(frame.getBytes)

    try {
      val it = JellyUtil.iterateRdfStream(inputStream)
        .zipWithIndex
      takeFrames.slice(it).foreach { case (maybeFrame, frameIndex) =>
        writeFrameToOutput(maybeFrame, frameIndex)
      }
    } finally {
      outputStream.flush()
    }
