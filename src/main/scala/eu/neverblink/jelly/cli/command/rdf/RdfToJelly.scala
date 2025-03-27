package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import com.google.protobuf.InvalidProtocolBufferException
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.RdfFormatOption.*
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.core.RdfProtoSerializationError
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{RDFLanguages, RDFParser, RiotException}

import java.io.{InputStream, OutputStream}

object RdfToJellyPrint extends RdfCommandPrintUtil:
  override val validFormats: List[RdfFormatOption] = List(NQuads)
  override val defaultFormat: RdfFormatOption = NQuads

case class RdfToJellyOptions(
    @Recurse
    common: JellyOptions = JellyOptions(),
    @ExtraName("to") outputFile: Option[String] = None,
    @ValueDescription("Input format.")
    @HelpMessage(
      RdfToJellyPrint.helpMsg,
    )
    @ExtraName("in-format") inputFormat: Option[String] = None,
) extends HasJellyOptions

object RdfToJelly extends JellyCommand[RdfToJellyOptions]:
  override def group = "rdf"

  override def names: List[List[String]] = List(
    List("rdf", "to-jelly"),
  )

  override def doRun(options: RdfToJellyOptions, remainingArgs: RemainingArgs): Unit =
    val (inputStream, outputStream) =
      getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    doConversion(inputStream, outputStream, options.inputFormat)

  /** This method takes care of proper error handling and matches the desired output format to the
    * correct conversion
    *
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    * @throws JellySerializationError
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
            case Some(NQuads) => nQuadToJelly(inputStream, outputStream)
            case _ =>
              throw InvalidFormatSpecified(
                f,
                RdfToJellyPrint.validFormatsString,
              ) // if anything else, it's an invalid option
        case None =>
          nQuadToJelly(inputStream, outputStream) // default option if no parameter supplied
      }
    } catch
      case e: RdfProtoSerializationError =>
        throw JellySerializationError(e.getMessage)
      case e: RiotException =>
        throw JenaRiotException(e)
      case e: InvalidProtocolBufferException =>
        throw InvalidJellyFile(e)

  /** This method reads the NQuad file, rewrites it to Jelly and writes it to some output stream
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    */
  private def nQuadToJelly(inputStream: InputStream, outputStream: OutputStream): Unit =
    val jellyWriter = StreamRDFWriter.getWriterStream(outputStream, JellyLanguage.JELLY)
    RDFParser.source(inputStream).lang(RDFLanguages.NQUADS).parse(jellyWriter)
