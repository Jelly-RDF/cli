package eu.neverblink.jelly.cli.command.rdf

import caseapp.{ExtraName, Recurse}
import caseapp.core.RemainingArgs
import eu.neverblink.jelly.cli.util.{FrameInfo, JellyUtil, MetricsPrinter}
import eu.neverblink.jelly.cli.{HasJellyCommandOptions, JellyCommand, JellyCommandOptions}
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
  // from what we've talked about yesterday it also sounded like we should accept some optional parameters
  // specifying which frames / parts of the stream to compute the metrics for
  // probably when only frameStart specified we should compute it only for this frame then

  override final def group = "rdf"

  override def doRun(options: RdfInspectOptions, remainingArgs: RemainingArgs): Unit =
    val (inputStream, outputStream) =
      this.getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    val printer = inspectJelly(inputStream)
    if options.perFrame then printer.printPerFrame(outputStream)
    else printer.printAggregate(outputStream)

  def inspectJelly(
      inputStream: InputStream,
  ): MetricsPrinter =
    val printer = new MetricsPrinter
    // Here we can easily compute overall metrics

    inline def computeMetrics(frame: RdfStreamFrame, frameIndex: Int): Unit =
      if printer.printOptions.isEmpty then
        if frame.rows.nonEmpty && frame.rows.head.row.isOptions then
          printer.printOptions = Some(frame.rows.head.row.asInstanceOf[RdfStreamOptions])
        else throw new RuntimeException("First row of the frame is not an options row")
      val metrics = new FrameInfo()
      frame.rows.foreach(r => metricsForRow(r, metrics))
      printer.frameInfo += metrics

    try {
      JellyUtil.iterateRdfStream(inputStream).zipWithIndex.foreach {
        case (maybeFrame, frameIndex) => computeMetrics(maybeFrame, frameIndex)
      }
      printer
    } catch {
      case e: Exception =>
        throw new RuntimeException("Error inspecting Jelly file", e)
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
