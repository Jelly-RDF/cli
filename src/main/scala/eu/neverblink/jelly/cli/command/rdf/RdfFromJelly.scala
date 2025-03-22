package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import eu.neverblink.jelly.cli.{
  HasJellyOptions,
  JellyCommand,
  JellyDeserializationError,
  JellyOptions,
  ParsingError,
}
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.core.RdfProtoDeserializationError
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{RDFLanguages, RDFParser}

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

  override def run(options: RdfFromJellyOptions, remainingArgs: RemainingArgs): Unit =
    super.setUpGeneralArgs(options, remainingArgs)
    val inputStream = remainingArgs.remaining.headOption match {
      case Some(fileName: String) =>
        Ops.readInputFile(fileName)
      case _ => System.in
    }
    val outputStream = options.outputFile match {
      case Some(fileName: String) =>
        Ops.createOutputStream(fileName)
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
      case e: Exception =>
        throw ParsingError(e.getMessage)
