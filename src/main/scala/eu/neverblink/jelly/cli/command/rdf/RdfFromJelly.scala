package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import com.google.protobuf.InvalidProtocolBufferException
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.util.IoUtil
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
import eu.ostrzyciel.jelly.core.{IoUtils, RdfProtoDeserializationError}
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{RDFLanguages, RDFParser, RiotException}

import java.io.{InputStream, OutputStream}

case class RdfFromJellyOptions(
    @Recurse
    common: JellyOptions = JellyOptions(),
    @ExtraName("to") outputFile: Option[String] = None,
    @ExtraName("out-format") outputFormat: Option[String] = None,
) extends HasJellyOptions

object RdfFromJelly extends JellyCommand[RdfFromJellyOptions]:
  override def group = "rdf"

  override def names: List[List[String]] = List(
    List("rdf", "from-jelly"),
  )

  // We exclude JellyBinary because translating JellyBinary to JellyBinary makes no sense
  private val correctOutputFormats =
    RdfFormatOptions.values.filterNot(_ == RdfFormatOptions.JellyBinary).map(f =>
      f.cliOption,
    ).toList

  override def doRun(options: RdfFromJellyOptions, remainingArgs: RemainingArgs): Unit =
    val inputStream = remainingArgs.remaining.headOption match {
      case Some(fileName: String) =>
        IoUtil.inputStream(fileName)
      case _ => System.in
    }
    val outputStream = options.outputFile match {
      case Some(fileName: String) =>
        IoUtil.outputStream(fileName)
      case None => getStdOut
    }
    doConversion(inputStream, outputStream, options.outputFormat)

  /** This method takes care of proper error handling and matches the desired output format to the
    * correct conversion
    *
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    * @throws JellyDeserializationError
    * @throws ParsingError
    * @throws InvalidFormatSpecified
    */
  private def doConversion(
      inputStream: InputStream,
      outputStream: OutputStream,
      format: Option[String],
  ): Unit =
    try {
      format match {
        case Some(RdfFormatOptions.JellyText.cliOption) =>
          JellyBinaryToText(inputStream, outputStream)
        case Some(RdfFormatOptions.NQuads.cliOption) => JellyToNQuad(inputStream, outputStream)
        case None =>
          JellyToNQuad(inputStream, outputStream) // default option if no parameter supplied
        case _ =>
          throw InvalidFormatSpecified(
            format.get,
            this.correctOutputFormats,
          ) // if anything else, it's an invalid option
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
  private def JellyToNQuad(inputStream: InputStream, outputStream: OutputStream): Unit =
    val nQuadWriter = StreamRDFWriter.getWriterStream(outputStream, RDFLanguages.NQUADS)
    RDFParser.source(inputStream).lang(JellyLanguage.JELLY).parse(nQuadWriter)

  /** This method reads the Jelly file, rewrites it to Jelly text and writes it to some output
    * stream
    *
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    */
  private def JellyBinaryToText(inputStream: InputStream, outputStream: OutputStream): Unit =
    inline def writeFrameToOutput(f: RdfStreamFrame, frameIndex: Int): Unit =
      // we want to write a comment to the file after each frame
      val comment = f"# Frame $frameIndex\n"
      outputStream.write(comment.getBytes)
      val frame = f.toProtoString
      // the protoString is basically the jelly-txt format already
      outputStream.write(frame.getBytes)

    try {
      IoUtils.autodetectDelimiting(inputStream) match
        case (false, newIn) =>
          // Non-delimited Jelly file
          // In this case, we can only read one frame
          val frame = RdfStreamFrame.parseFrom(newIn)
          writeFrameToOutput(frame, 0)
        case (true, newIn) =>
          // Delimited Jelly file
          // In this case, we can read multiple frames
          Iterator.continually(RdfStreamFrame.parseDelimitedFrom(newIn))
            .takeWhile(_.isDefined).zipWithIndex
            .foreach { case (maybeFrame, frameIndex) =>
              writeFrameToOutput(maybeFrame.get, frameIndex)
            }
    } finally {
      outputStream.flush()
    }

  def getCorrectOutputFormats: List[String] = correctOutputFormats
