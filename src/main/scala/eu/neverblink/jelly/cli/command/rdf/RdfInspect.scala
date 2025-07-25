package eu.neverblink.jelly.cli.command.rdf

import caseapp.{ArgsName, ExtraName, HelpMessage, Recurse}
import caseapp.core.RemainingArgs
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.*
import eu.neverblink.jelly.core.proto.v1.*

import scala.jdk.CollectionConverters.*

import java.io.InputStream
@HelpMessage(
  "Prints statistics about a Jelly-RDF stream.\n" +
    "Statistics include: Jelly stream options and counts/sizes of various row types, " +
    "including triples, quads, names, prefixes, " +
    "namespaces, datatypes, and graphs.\n" +
    "Output statistics are returned as a valid YAML. \n" +
    "If no input file is specified, the input is read from stdin.\n" +
    "If no output file is specified, the output is written to stdout.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n" +
    "Otherwise, the program will exit with code 0.\n",
  "Note: this command works in a streaming manner and scales well to large files",
)
@ArgsName("<file-to-inspect>")
case class RdfInspectOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @HelpMessage(
      "File to write the output statistics to. If not specified, the output is written to stdout.",
    )
    @ExtraName("to") outputFile: Option[String] = None,
    @HelpMessage(
      "Whether to print the statistics per frame (default: false). " +
        "If true, the statistics are computed and printed separately for each frame in the stream.",
    )
    perFrame: Boolean = false,
    @HelpMessage(
      "Control the detailed output. One of 'node', 'term', 'all'. " +
        "Groups output by node type ('node'), subject/predicate/object " +
        "term position ('term'), or doesn't aggregate ('all').",
    )
    detail: Option[String] = None,
    @HelpMessage(
      "Report the size (in bytes) of rows and other elements, rather than their counts.",
    )
    size: Boolean = false,
) extends HasJellyCommandOptions

object RdfInspect extends JellyCommand[RdfInspectOptions]:

  override def names: List[List[String]] = List(
    List("rdf", "inspect"),
  )

  override final def group = "rdf"

  override def doRun(options: RdfInspectOptions, remainingArgs: RemainingArgs): Unit =
    val (inputStream, outputStream) =
      this.getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    val formatter = options.detail match {
      case Some("all") => MetricsPrinter.allFormatter
      case Some("node") => MetricsPrinter.nodeGroupFormatter
      case Some("term") => MetricsPrinter.termGroupFormatter
      case Some(value) =>
        throw InvalidArgument("--detail", value, Some("Must be one of 'all', 'node', 'term'"))
      case None => MetricsPrinter.allFormatter
    }
    val statCollector = if options.size then FrameInfo.SizeStatistic else FrameInfo.CountStatistic
    given FrameInfo.StatisticCollector = statCollector
    val (streamOpts, frameIterator) = inspectJelly(inputStream, options.detail.isDefined)
    val metricsPrinter = new MetricsPrinter(formatter)
    if options.perFrame then metricsPrinter.printPerFrame(streamOpts, frameIterator, outputStream)
    else metricsPrinter.printAggregate(streamOpts, frameIterator, outputStream)

  private def inspectJelly(
      inputStream: InputStream,
      detail: Boolean,
  )(using FrameInfo.StatisticCollector): (RdfStreamOptions, Iterator[FrameInfo]) =

    inline def computeMetrics(
        frame: RdfStreamFrame,
        frameIndex: Int,
    ): FrameInfo =
      val metadata = frame.getMetadata.asScala.map(entry => entry.getKey -> entry.getValue).toMap
      val metrics =
        if detail then new FrameDetailInfo(frameIndex, metadata)
        else FrameInfo(frameIndex, metadata)
      metrics.processFrame(frame)
      metrics

    try {
      val allRows = JellyUtil.iterateRdfStream(inputStream).buffered
      // we need to check if the first frame contains options
      val streamOptions = checkOptions(allRows.headOption)
      // We compute the metrics for each frame
      // and then sum them all during the printing if desired
      val frameIterator = allRows.zipWithIndex.map { case (maybeFrame, frameIndex) =>
        computeMetrics(maybeFrame, frameIndex)
      }
      (streamOptions, frameIterator)
    } catch {
      case e: Exception =>
        throw InvalidJellyFile(e)
    }

  /** Checks whether the first frame in the stream contains options and returns them.
    * @param headFrame
    *   The first frame in the stream as an option.
    * @return
    *   The options from the first frame.
    * @throws RuntimeException
    *   If the first frame does not contain options or if there are no frames in the stream.
    */
  private def checkOptions(headFrame: Option[RdfStreamFrame]): RdfStreamOptions =
    if headFrame.isEmpty then throw new RuntimeException("No frames in the stream.")
    if headFrame.get.getRows.asScala.isEmpty then
      throw new RuntimeException("No rows in the frame.")
    val frameRows = headFrame.get.getRows.asScala
    frameRows.head.getRow match {
      case r: RdfStreamOptions => r
      case _ => throw new RuntimeException("First row of the frame is not an options row.")
    }
