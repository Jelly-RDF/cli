package eu.neverblink.jelly.cli.command.rdf

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.*
import eu.neverblink.jelly.cli.util.args.IndexRange
import eu.neverblink.jelly.cli.util.io.IoUtil
import eu.neverblink.jelly.cli.util.jena.*
import eu.neverblink.jelly.convert.jena.JenaConverterFactory
import eu.neverblink.jelly.core.JellyOptions
import eu.neverblink.jelly.core.RdfHandler.AnyStatementHandler
import eu.neverblink.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.RDFParser
import org.apache.jena.riot.system.StreamRDFLib
import org.apache.jena.sparql.core.Quad

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.util.Using

object RdfValidatePrint extends RdfCommandPrintUtil[RdfFormat.Jena]:
  override val defaultFormat: RdfFormat = RdfFormat.NQuads

@HelpMessage(
  "Validates a Jelly-RDF stream.\nIf no additional options are specified, " +
    "only basic validations are performed. You can also validate the stream against " +
    "a reference RDF file, check the stream options, and its delimiting.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n" +
    "Otherwise, the program will exit with code 0.\n" +
    "Note: this command does not work in a streaming manner. If you try to validate a very large " +
    "file, you may run out of memory.",
)
@ArgsName("<file-to-validate>")
case class RdfValidateOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @HelpMessage(
      "RDF file to compare the input stream to. If not specified, no comparison is done.",
    )
    compareToRdfFile: Option[String] = None,
    @HelpMessage(
      "Format of the RDF file to compare the input stream to. If not specified, the format is " +
        "inferred from the file name. " + RdfValidatePrint.validFormatsString,
    )
    compareToFormat: Option[String] = None,
    @HelpMessage(
      "Whether the comparison should be ordered (statements must come in a specific order) or " +
        "unordered (RDF dataset isomorphism). Default: false (unordered)",
    )
    compareOrdered: Boolean = false,
    @HelpMessage(
      "Frame indices to compare. If not specified, all frames are compared. " +
        IndexRange.helpText,
    )
    compareFrameIndices: String = "",
    @HelpMessage(
      "File with the expected stream options. If not specified, the options are not checked.",
    )
    optionsFile: Option[String] = None,
    @HelpMessage(
      "Whether the input stream should be checked to be delimited or undelimited. " +
        "Possible values: 'either', 'true', 'false'. Default: 'either'.",
    )
    delimited: String = "either",
) extends HasJellyCommandOptions

object RdfValidate extends JellyCommand[RdfValidateOptions]:
  private enum Delimiting:
    case Either, Delimited, Undelimited

  override def names: List[List[String]] = List(List("rdf", "validate"))

  override def group = "rdf"

  override def doRun(options: RdfValidateOptions, remainingArgs: RemainingArgs): Unit =
    // Parse input options
    val frameIndices = IndexRange(options.compareFrameIndices, "--compare-frame-indices")
    val delimiting = options.delimited match
      case "" | "either" => Delimiting.Either
      case "true" => Delimiting.Delimited
      case "false" => Delimiting.Undelimited
      case _ =>
        throw InvalidArgument(
          "--delimited",
          options.delimited,
          Some("Valid values: true, false, either"),
        )
    val rdfComparison =
      options.compareToRdfFile.map(n => getRdfForComparison(n, options.compareToFormat))
    val (inputStream, _) = getIoStreamsFromOptions(remainingArgs.remaining.headOption, None)
    val (delimited, frameIterator) = JellyUtil.iterateRdfStreamWithDelimitingInfo(inputStream)

    // Step 1: Validate delimiting
    validateDelimiting(delimiting, delimited)
    // Step 2: Validate basic stream structure & the stream options
    val framesSeq = frameIterator.toSeq
    val streamOptions = validateOptions(skipEmptyFrames(framesSeq))
    // Step 3: Validate the content
    validateContent(framesSeq, frameIndices, rdfComparison, streamOptions)

  private def validateDelimiting(
      expected: Delimiting,
      delimited: Boolean,
  ): Unit = expected match
    case Delimiting.Either => ()
    case Delimiting.Delimited =>
      if !delimited then
        throw CriticalException("Expected delimited input, but the file was not delimited")
    case Delimiting.Undelimited =>
      if delimited then
        throw CriticalException("Expected undelimited input, but the file was delimited")

  private def validateOptions(frames: Seq[RdfStreamFrame]): RdfStreamOptions =
    if !frames.head.getRows.asScala.head.hasOptions then
      throw CriticalException("First row in the input stream does not contain stream options")
    val streamOptions = frames.head.getRows.asScala.head.getOptions
    // If we have expected options, we need to read and validate them
    val expectedOptions = getOptions.optionsFile.map { optionsFileName =>
      val o = Using.resource(IoUtil.inputStream(optionsFileName)) { is =>
        JellyUtil.iterateRdfStream(is).next().getRows.asScala.head.getOptions
      }
      if streamOptions != o then
        throw CriticalException(
          s"Stream options do not match the expected options in $optionsFileName\n" +
            s"Expected: ${StreamOptionsUtil.prettyPrint(o)}\n" +
            s"Actual: ${StreamOptionsUtil.prettyPrint(streamOptions)}",
        )
      o
    }
    if streamOptions.getVersion <= 0 then
      throw CriticalException(
        "The version field in RdfStreamOptions is <= 0. This field MUST be set to a positive value.",
      )
    JellyOptions.checkCompatibility(
      streamOptions,
      expectedOptions.getOrElse(JellyOptions.DEFAULT_SUPPORTED_OPTIONS),
    )
    streamOptions

  private def validateContent(
      frames: Seq[RdfStreamFrame],
      frameIndices: IndexRange,
      maybeRdfComparison: Option[StreamRdfCollector],
      opt: RdfStreamOptions,
  ): Unit =
    // Prepare data structures
    val jellyStreamConsumer =
      if maybeRdfComparison.isDefined then StreamRdfCollector()
      else StreamRDFLib.sinkNull()

    val startFrom = frameIndices.start.getOrElse(0)
    var currentPosition = 0

    val handler = new AnyStatementHandler[Node] {
      override def handleNamespace(prefix: String, namespace: Node): Unit =
        jellyStreamConsumer.prefix(prefix, namespace.getURI)

      override def handleTriple(subject: Node, predicate: Node, `object`: Node): Unit = {
        val t = Triple.create(subject, predicate, `object`)
        if !opt.getGeneralizedStatements && StatementUtils.isGeneralized(t) then
          throw CriticalException(s"Unexpected generalized triple in frame $currentPosition: $t")
        if !opt.getRdfStar && StatementUtils.isRdfStar(t) then
          throw CriticalException(s"Unexpected RDF-star triple in frame $currentPosition: $t")
        // Add the triple to the comparison set, if we are in the compare range
        if currentPosition >= startFrom then jellyStreamConsumer.triple(t)
      }

      override def handleQuad(subject: Node, predicate: Node, `object`: Node, graph: Node): Unit = {
        val q = new Quad(graph, subject, predicate, `object`)
        if !opt.getGeneralizedStatements && StatementUtils.isGeneralized(q) then
          throw CriticalException(s"Unexpected generalized quad in frame $currentPosition: $q")
        if !opt.getRdfStar && StatementUtils.isRdfStar(q) then
          throw CriticalException(s"Unexpected RDF-star quad in frame $currentPosition: $q")
        // Add the quad to the comparison set, if we are in the compare range
        if currentPosition >= startFrom then jellyStreamConsumer.quad(q)
      }
    }
    val dec = JenaConverterFactory.getInstance().anyStatementDecoder(
      handler,
      JellyOptions.DEFAULT_SUPPORTED_OPTIONS,
    )
    val frames2 = frameIndices.end match
      case Some(end) => frames.take(end)
      case None => frames

    for (frame, i) <- frames2.zipWithIndex do
      for row <- frame.getRows.asScala do
        if row.hasOptions && row.getOptions != opt then
          throw CriticalException(
            s"Later occurrence of stream options in frame $i does not match the first",
          )
        // Push the stream frames through the decoder
        // This will catch most of the errors
        currentPosition = i
        dec.ingestRow(row)
    // Compare the Jelly data with the reference RDF data, if specified
    maybeRdfComparison.foreach { rdfComparison =>
      val actual = jellyStreamConsumer.asInstanceOf[StreamRdfCollector]
      val comparator =
        if getOptions.compareOrdered then OrderedRdfCompare
        else UnorderedRdfCompare
      comparator.compare(rdfComparison, actual)
    }

  /** Skip empty frames in the stream. If the first frame is empty, we skip it and continue with the
    * next one. If the first row is empty, we throw an exception
    * @param frames
    *   frames to check
    * @return
    *   frames after empty frames
    */
  @tailrec
  private def skipEmptyFrames(
      frames: Seq[RdfStreamFrame],
  ): Seq[RdfStreamFrame] =
    if frames.isEmpty then throw CriticalException("Empty input stream")
    else if frames.head.getRows.isEmpty then
      // We want to accept empty frames in the stream, but not empty streams
      if frames.tail.isEmpty then throw CriticalException("All frames are empty")
      skipEmptyFrames(frames.tail)
    else frames

  /** Reads the RDF file for comparison and returns a StreamRdfCollector
    * @param fileName
    *   filename to read
    * @param formatName
    *   optional format name
    * @return
    */
  private def getRdfForComparison(
      fileName: String,
      formatName: Option[String],
  ): StreamRdfCollector =
    val explicitFormat = formatName.flatMap(RdfFormat.find)
    val implicitFormat = RdfFormat.inferFormat(fileName)
    val format = (explicitFormat, implicitFormat) match {
      case (Some(f: RdfFormat.Jena), _) => f
      case (_, Some(f: RdfFormat.Jena)) => f
      case (_, _) =>
        throw InvalidFormatSpecified(
          formatName.getOrElse(""),
          RdfValidatePrint.validFormatsString,
        )
    }
    val output = StreamRdfCollector()
    Using.resource(IoUtil.inputStream(fileName)) { is =>
      RDFParser.source(is)
        .lang(format.jenaLang)
        .parse(output)
    }
    output
