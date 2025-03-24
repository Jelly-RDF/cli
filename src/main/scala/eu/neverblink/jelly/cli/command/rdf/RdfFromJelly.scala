package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import com.google.protobuf.InvalidProtocolBufferException
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.util.IoUtil
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.core.RdfProtoDeserializationError
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{RDFLanguages, RDFParser, RiotException}

import java.io.{InputStream, OutputStream}

case class RdfFromJellyOptions(
    @Recurse
    common: JellyOptions = JellyOptions(),
    @ExtraName("to") outputFile: Option[String] = None,
) extends HasJellyOptions

object RdfFromJelly extends JellyCommand[RdfFromJellyOptions]:
  override def group = "rdf"

  override def names: List[List[String]] = List(
    List("rdf", "from-jelly"),
  )

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
    doConversion(inputStream, outputStream)

  /** This method reads the Jelly file, rewrites it to NQuads and writes it to some output stream
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    * @throws JellyDeserializationError
    * @throws ParsingError
    */
  private def doConversion(inputStream: InputStream, outputStream: OutputStream): Unit =
    try {
      val nQuadWriter = StreamRDFWriter.getWriterStream(outputStream, RDFLanguages.NQUADS)
      RDFParser.source(inputStream).lang(JellyLanguage.JELLY).parse(nQuadWriter)
    } catch
      case e: RdfProtoDeserializationError =>
        throw JellyDeserializationError(e.getMessage)
      case e: RiotException =>
        throw JenaRiotException(e)
      case e: InvalidProtocolBufferException =>
        throw InvalidJellyFile(e)
