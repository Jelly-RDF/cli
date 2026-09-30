package eu.neverblink.jelly.cli.command.sparql

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.FrameInfo
import eu.neverblink.jelly.cli.command.sparql.util.*

import scala.jdk.CollectionConverters.*

@HelpMessage(
  "Prints statistics about a Jelly-SPARQL stream.\n" +
    "The statistics are returned as a valid YAML. \n" +
    "If no input file is specified, the input is read from stdin.\n" +
    "If no output file is specified, the output is written to stdout.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n",
  "Note: this command works in a streaming manner and scales well to large files. " +
    "It only reads the structure of the frames, it does not decode the RDF terms – " +
    "use sparql validate for that.",
)
@ArgsName("<file-to-inspect>")
case class SparqlInspectOptions(
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
      "Report the size (in bytes) of frames and other elements, rather than their counts.",
    )
    size: Boolean = false,
) extends HasJellyCommandOptions

object SparqlInspect extends JellyCommand[SparqlInspectOptions]:

  override def names: List[List[String]] = List(
    List("sparql", "inspect"),
  )

  override final def group = "sparql"

  override def doRun(options: SparqlInspectOptions, remainingArgs: RemainingArgs): Unit =
    val (inputStream, outputStream) =
      getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    given FrameInfo.StatisticCollector =
      if options.size then FrameInfo.SizeStatistic else FrameInfo.CountStatistic
    SparqlJellyUtil.translateErrors {
      val frames = SparqlJellyUtil.iterateSparqlStream(inputStream).buffered
      if !frames.hasNext then throw CriticalException("Empty input stream")
      val firstFrame = frames.head
      if firstFrame.getOptions == null then
        throw CriticalException("First frame in the input stream does not contain stream options")
      val frameInfos = frames.zipWithIndex.map { (frame, i) =>
        val metadata = frame.getMetadata.asScala.map(e => e.getKey -> e.getValue).toMap
        val info = SparqlFrameInfo(i, metadata)
        info.processFrame(frame)
        info
      }
      if options.perFrame then
        SparqlMetricsPrinter.printPerFrame(firstFrame, frameInfos, outputStream)
      else SparqlMetricsPrinter.printAggregate(firstFrame, frameInfos, outputStream)
    }
