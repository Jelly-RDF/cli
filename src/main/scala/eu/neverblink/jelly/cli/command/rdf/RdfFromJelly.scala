package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.RdfFormat.*
import eu.neverblink.jelly.cli.command.rdf.RdfFormat.Jena.*
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
import eu.ostrzyciel.jelly.core.IoUtils
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{Lang, RDFParser}

import java.io.{InputStream, OutputStream}

object RdfFromJellyPrint extends RdfCommandPrintUtil[RdfFormat.Writeable]:
  override val defaultFormat: RdfFormat = RdfFormat.NQuads

case class RdfFromJellyOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @ExtraName("to") outputFile: Option[String] = None,
    @ValueDescription("Output format.")
    @HelpMessage(
      RdfFromJellyPrint.helpMsg,
    )
    @ExtraName("out-format") outputFormat: Option[String] = None,
) extends HasJellyCommandOptions

object RdfFromJelly extends RdfCommand[RdfFromJellyOptions, RdfFormat.Writeable]:

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
      option: RdfFormat.Writeable,
  ): Option[(InputStream, OutputStream) => Unit] =
    option match
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
      iterateRdfStream(inputStream, outputStream).zipWithIndex.foreach {
        case (maybeFrame, frameIndex) =>
          writeFrameToOutput(maybeFrame, frameIndex)
      }
    } finally {
      outputStream.flush()
    }

  /** This method reads the Jelly file and returns an iterator of RdfStreamFrame
    * @param inputStream
    * @param outputStream
    * @return
    */
  private def iterateRdfStream(
      inputStream: InputStream,
      outputStream: OutputStream,
  ): Iterator[RdfStreamFrame] =
    IoUtils.autodetectDelimiting(inputStream) match
      case (false, newIn) =>
        // Non-delimited Jelly file
        // In this case, we can only read one frame
        Iterator(RdfStreamFrame.parseFrom(newIn))
      case (true, newIn) =>
        // Delimited Jelly file
        // In this case, we can read multiple frames
        Iterator.continually(RdfStreamFrame.parseDelimitedFrom(newIn))
          .takeWhile(_.isDefined).map(_.get)
