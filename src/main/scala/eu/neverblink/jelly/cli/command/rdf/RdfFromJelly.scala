package eu.neverblink.jelly.cli.command.rdf

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.*
import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat.*
import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat.Jena.*
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{Lang, RDFParser}

import java.io.{InputStream, OutputStream}

object RdfFromJellyPrint extends RdfCommandPrintUtil[RdfFormat.Writeable]:
  override val defaultFormat: RdfFormat = RdfFormat.NQuads

@HelpMessage(
  "Translates a Jelly-RDF stream to a different RDF format. \n" +
    "If not input file is specified, the input is read from stdin.\n" +
    "If no output file is specified, the output is written to stdout.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n" +
    "Otherwise, the program will exit with code 0.\n" +
    "Note: this command works in a streaming manner and scales well to large files",
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
) extends HasJellyCommandOptions

object RdfFromJelly extends RdfTranscodeCommand[RdfFromJellyOptions, RdfFormat.Writeable]:

  override def names: List[List[String]] = List(
    List("rdf", "from-jelly"),
  )

  lazy val printUtil: RdfCommandPrintUtil[RdfFormat.Writeable] = RdfFromJellyPrint

  val defaultAction: (InputStream, OutputStream) => Unit =
    jellyToLang(RdfFormat.NQuads.jenaLang, _, _)

  override def doRun(options: RdfFromJellyOptions, remainingArgs: RemainingArgs): Unit =
    val (inputStream, outputStream) =
      this.getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    parseFormatArgs(inputStream, outputStream, options.outputFormat, options.outputFile)

  override def matchFormatToAction(
      format: RdfFormat.Writeable,
  ): Option[(InputStream, OutputStream) => Unit] =
    format match
      case j: RdfFormat.Jena.Writeable => Some(jellyToLang(j.jenaLang, _, _))
      case RdfFormat.JellyText => Some(jellyBinaryToText)

  /** This method reads the Jelly file, rewrites it to specified format and writes it to some output
    * stream
    * @param jenaLang
    *   Language that jelly should be converted to
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    */
  private def jellyToLang(
      jenaLang: Lang,
      inputStream: InputStream,
      outputStream: OutputStream,
  ): Unit =
    val nQuadWriter = StreamRDFWriter.getWriterStream(outputStream, jenaLang)
    RDFParser.source(inputStream).lang(JellyLanguage.JELLY).parse(nQuadWriter)

  /** This method reads the Jelly file, rewrites it to Jelly text and writes it to some output
    * stream
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    */
  private def jellyBinaryToText(inputStream: InputStream, outputStream: OutputStream): Unit =

    inline def writeFrameToOutput(f: RdfStreamFrame, frameIndex: Int): Unit =
      // we want to write a comment to the file before each frame
      val comment = f"# Frame $frameIndex\n"
      outputStream.write(comment.getBytes)
      val frame = f.toProtoString
      // the protoString is basically the jelly-txt format already
      outputStream.write(frame.getBytes)

    try {
      JellyUtil.iterateRdfStream(inputStream).zipWithIndex.foreach {
        case (maybeFrame, frameIndex) =>
          writeFrameToOutput(maybeFrame, frameIndex)
      }
    } finally {
      outputStream.flush()
    }
