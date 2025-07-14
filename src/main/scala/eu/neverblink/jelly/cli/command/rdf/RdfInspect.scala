package eu.neverblink.jelly.cli.command.rdf

import caseapp.{ArgsName, ExtraName, HelpMessage, Recurse}
import caseapp.core.RemainingArgs
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.{FrameInfo, JellyUtil, MetricsPrinter}
import eu.neverblink.jelly.core.proto.v1.*

import scala.jdk.CollectionConverters.*

import java.io.InputStream
@HelpMessage(
  "Prints statistics about a Jelly-RDF stream.\n" +
    "Statistics include: Jelly stream options and counts of various row types, " +
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
) extends HasJellyCommandOptions

object RdfInspect extends JellyCommand[RdfInspectOptions]:

  override def names: List[List[String]] = List(
    List("rdf", "inspect"),
  )

  override final def group = "rdf"

  override def doRun(options: RdfInspectOptions, remainingArgs: RemainingArgs): Unit =
    val (inputStream, outputStream) =
      this.getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    val (streamOpts, frameIterator) = inspectJelly(inputStream)
    if options.perFrame then MetricsPrinter.printPerFrame(streamOpts, frameIterator, outputStream)
    else MetricsPrinter.printAggregate(streamOpts, frameIterator, outputStream)

  private def inspectJelly(
      inputStream: InputStream,
  ): (RdfStreamOptions, Iterator[FrameInfo]) =

    inline def computeMetrics(
        frame: RdfStreamFrame,
        frameIndex: Int,
    ): FrameInfo =
      val metrics = new FrameInfo(
        frameIndex,
        frame.getMetadata.asScala.map(entry => entry.getKey -> entry.getValue).toMap,
      )
      frame.getRows.asScala.foreach(r => metricsForRow(r, metrics))
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

  private def metricsForRow(
      row: RdfStreamRow,
      metadata: FrameInfo,
  ): Unit = metadata.processStreamRow(row)

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
