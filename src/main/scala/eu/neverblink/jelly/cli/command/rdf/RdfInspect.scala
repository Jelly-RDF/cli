package eu.neverblink.jelly.cli.command.rdf

import caseapp.{ExtraName, Recurse}
import caseapp.core.RemainingArgs
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.{FrameInfo, JellyUtil, MetricsPrinter}
import eu.ostrzyciel.jelly.core.proto.v1.*

import java.io.InputStream

case class RdfInspectOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @ExtraName("to") outputFile: Option[String] = None,
    @ExtraName("per-frame") perFrame: Boolean = false,
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
      val metrics = new FrameInfo(frameIndex)
      frame.rows.foreach(r => metricsForRow(r, metrics))
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
  ): Unit =
    row.row match {
      case r: RdfTriple => metadata.tripleCount += 1
      case r: RdfQuad => metadata.quadCount += 1
      case r: RdfNameEntry => metadata.nameCount += 1
      case r: RdfPrefixEntry => metadata.prefixCount += 1
      case r: RdfNamespaceDeclaration => metadata.namespaceCount += 1
      case r: RdfDatatypeEntry => metadata.datatypeCount += 1
      case r: RdfGraphStart => metadata.graphStartCount += 1
      case r: RdfGraphEnd => metadata.graphEndCount += 1
      case r: RdfStreamOptions => metadata.optionCount += 1
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
    if headFrame.get.rows.isEmpty then throw new RuntimeException("No rows in the frame.")
    val frameRows = headFrame.get.rows
    frameRows.head.row match {
      case r: RdfStreamOptions => r
      case _ => throw new RuntimeException("First row of the frame is not an options row.")
    }
