package eu.neverblink.jelly.cli.command.rdf

import caseapp.{ExtraName, Recurse}
import caseapp.core.RemainingArgs
import eu.neverblink.jelly.cli.util.{FrameInfo, JellyUtil, MetricsPrinter}
import eu.neverblink.jelly.cli.*
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
    val printer = inspectJelly(inputStream)
    if options.perFrame then printer.printPerFrame(outputStream)
    else printer.printAggregate(outputStream)

  private def inspectJelly(
      inputStream: InputStream,
  ): MetricsPrinter =

    inline def computeMetrics(
        frame: RdfStreamFrame,
        frameIndex: Int,
        printer: MetricsPrinter,
    ): Unit =
      val metrics = new FrameInfo()
      frame.rows.foreach(r => metricsForRow(r, metrics))
      printer.frameInfo += metrics

    try {
      val allRows = JellyUtil.iterateRdfStream(inputStream).toList
      // we need to check if the first frame contains options
      val streamOptions = checkOptions(allRows)
      val printer = new MetricsPrinter(streamOptions)
      // We compute the metrics for each frame
      // and then sum them all during the printing if desired
      allRows.zipWithIndex.foreach { case (maybeFrame, frameIndex) =>
        computeMetrics(maybeFrame, frameIndex, printer)
      }
      printer
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
    * @param allFrames
    *   The list of all frames in the stream.
    * @return
    *   The options from the first frame.
    * @throws RuntimeException
    *   If the first frame does not contain options or if there are no frames in the stream.
    */
  private def checkOptions(allFrames: List[RdfStreamFrame]): RdfStreamOptions =
    if allFrames.isEmpty then throw new RuntimeException("No frames in the stream.")
    if allFrames.head.rows.isEmpty then throw new RuntimeException("No rows in the frame.")
    val frameRows = allFrames.head.rows
    frameRows.head.row match {
      case r: RdfStreamOptions => r
      case _ => throw new RuntimeException("First row of the frame is not an options row.")
    }
