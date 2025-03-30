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
    doConversion(inputStream, outputStream, options.outputFormat, options.outputFile)

  /** This method takes care of proper error handling and matches the desired output format to the
    * correct conversion
    *
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    * @param format
    *   Option[String]
    * @param fileName
    *   Option[String]
    * @throws JellyDeserializationError
    * @throws JenaRiotException
    * @throws InvalidJellyFile
    */
  private def doConversion(
      inputStream: InputStream,
      outputStream: OutputStream,
      format: Option[String],
      fileName: Option[String],
  ): Unit =
    try {
      val explicitFormat = if (format.isDefined) RdfFormatOption.find(format.get) else None
      val implicitFormat =
        if (fileName.isDefined) RdfFormatOption.inferFormat(fileName.get) else None
      (explicitFormat, implicitFormat) match {
        case (Some(f: RdfFormatOption), _) if matchToAction(Some(f)).isDefined =>
          matchToAction(Some(f)).get(inputStream, outputStream)
        // If format explicitely defined but does not match any available format, we throw an error
        case (None, _) if format.isDefined =>
          throw InvalidFormatSpecified(format.get, RdfFromJellyPrint.validFormatsString)
        case (_, Some(f: RdfFormatOption)) if matchToAction(Some(f)).isDefined =>
          matchToAction(Some(f)).get(inputStream, outputStream)
        // If format not explicitely defined but implicitely not understandable we default to this
        case (_, _) => jellyToNQuad(inputStream, outputStream)
      }
    } catch
      case e: RdfProtoDeserializationError =>
        throw JellyDeserializationError(e.getMessage)
      case e: RiotException =>
        throw JenaRiotException(e)
      case e: InvalidProtocolBufferException =>
        throw InvalidJellyFile(e)

  private def matchToAction(
      option: Option[RdfFormatOption],
  ): Option[(InputStream, OutputStream) => Unit] =
    option match
      case Some(JellyText) => Some(jellyBinaryToText)
      case Some(NQuads) => Some(jellyToNQuad)
      case _ => None
  // throw InvalidFormatSpecified(ogParameter, RdfFromJellyPrint.validFormatsString)

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
