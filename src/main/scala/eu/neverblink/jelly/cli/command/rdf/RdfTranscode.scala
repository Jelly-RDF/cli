package eu.neverblink.jelly.cli.command.rdf

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.*
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamOptions
import eu.ostrzyciel.jelly.core.{JellyOptions, ProtoTranscoder}

import java.io.{InputStream, OutputStream}

@HelpMessage(
  "Quickly transcodes the input Jelly file into another Jelly file.\n" +
    "If no input file is specified, the input is read from stdin.\n" +
    "The input may be a concatenation of multiple Jelly streams.\n" +
    "Currently only frame-by-frame transcoding is supported.\n" +
    "The output's options must be greater than or equal to the input options.\n" +
    "Note: this command works in a streaming manner and scales well to large files.",
)
@ArgsName("<file-to-transcode>")
case class RdfTranscodeOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @HelpMessage(
      "Output file to write the Jelly data to. If not specified, the output is written to stdout.",
    )
    @ExtraName("to") outputFile: Option[String] = None,
    @Recurse
    jellySerializationOptions: RdfJellySerializationOptions = RdfJellySerializationOptions(),
    // TODO: supported input options
    // TODO: make it possible to not only frame-by-frame transcode, but also regroup the rows
    // TODO: make it possible to do full transcoding (with Jena parsing)
) extends HasJellyCommandOptions

object RdfTranscode extends JellyCommand[RdfTranscodeOptions]:
  override def names: List[List[String]] = List(
    List("rdf", "transcode"),
  )

  override final def group = "rdf"

  override def doRun(options: RdfTranscodeOptions, remainingArgs: RemainingArgs): Unit =
    val outOpt = options.jellySerializationOptions.asRdfStreamOptions
    val (inputStream, outputStream) =
      getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    jellyToJelly(inputStream, outputStream, outOpt)

  /** Transcodes the input Jelly stream into another Jelly stream.
    * @param inputStream
    *   input
    * @param outputStream
    *   output
    * @param outOpt
    *   user-defined options for the output
    */
  private def jellyToJelly(
      inputStream: InputStream,
      outputStream: OutputStream,
      outOpt: RdfStreamOptions,
  ): Unit =
    val in = JellyUtil.iterateRdfStream(inputStream).buffered
    val head = in.head
    if head.rows.isEmpty then throw CriticalException("Empty input stream")
    if !head.rows.head.row.isOptions then
      throw CriticalException("First input row is not an options row")
    val inOpt = head.rows.head.row.options

    val transcoder = ProtoTranscoder.fastMergingTranscoder(
      supportedInputOptions = JellyOptions.defaultSupportedOptions,
      outputOptions = outOpt.copy(
        // There is no way to specify the physical type with options currently.
        // Just use the one from the input.
        physicalType = inOpt.physicalType,
        logicalType =
          if outOpt.logicalType.isUnspecified then inOpt.logicalType else outOpt.logicalType,
      ),
    )

    in.map(transcoder.ingestFrame)
      .foreach(_.writeDelimitedTo(outputStream))
