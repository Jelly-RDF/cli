package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import com.google.protobuf.InvalidProtocolBufferException
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.RdfFormatOption.*
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
import eu.ostrzyciel.jelly.core.{IoUtils, RdfProtoDeserializationError}
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{RDFLanguages, RDFParser, RiotException}

import java.io.{InputStream, OutputStream}

object RdfFromJellyPrint extends RdfCommandPrintUtil:
  override val validFormats: List[RdfFormatOption] = List(JellyText, NQuads)
  override val defaultFormat: RdfFormatOption = NQuads

case class RdfFromJellyOptions(
    @Recurse
    common: JellyOptions = JellyOptions(),
    @ExtraName("to") outputFile: Option[String] = None,
    @ValueDescription("Output format.")
    @HelpMessage(
      RdfFromJellyPrint.helpMsg,
    )
    @ExtraName("out-format") outputFormat: Option[String] = None,
) extends HasJellyOptions

object RdfFromJelly extends JellyCommand[RdfFromJellyOptions]:
  override def group = "rdf"

  override def names: List[List[String]] = List(
    List("rdf", "from-jelly"),
  )

  override def doRun(options: RdfFromJellyOptions, remainingArgs: RemainingArgs): Unit =
    val (inputStream, outputStream) =
      this.getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    doConversion(inputStream, outputStream, options.outputFormat)

  /** This method takes care of proper error handling and matches the desired output format to the
    * correct conversion
    *
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    * @throws JellyDeserializationError
    * @throws JenaRiotException
    * @throws InvalidJellyFile
    */
  private def doConversion(
      inputStream: InputStream,
      outputStream: OutputStream,
      format: Option[String],
  ): Unit =
    try {
      format match {
        case Some(f: String) =>
          RdfFormatOption.find(f) match
            case Some(JellyText) => jellyBinaryToText(inputStream, outputStream)
            case Some(NQuads) => jellyToNQuad(inputStream, outputStream)
            case _ =>
              throw InvalidFormatSpecified(
                f,
                RdfFromJellyPrint.validFormatsString,
              ) // if anything else, it's an invalid option
        case None =>
          jellyToNQuad(inputStream, outputStream) // default option if no parameter supplied
      }
    } catch
      case e: RdfProtoDeserializationError =>
        throw JellyDeserializationError(e.getMessage)
      case e: RiotException =>
        throw JenaRiotException(e)
      case e: InvalidProtocolBufferException =>
        throw InvalidJellyFile(e)

  /** This method reads the Jelly file, rewrites it to NQuads and writes it to some output stream
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    */
  private def jellyToNQuad(inputStream: InputStream, outputStream: OutputStream): Unit =
    val nQuadWriter = StreamRDFWriter.getWriterStream(outputStream, RDFLanguages.NQUADS)
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
