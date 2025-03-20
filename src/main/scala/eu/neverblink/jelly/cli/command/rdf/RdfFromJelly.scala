package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import eu.neverblink.jelly.cli.JellyCommand
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{RDFLanguages, RDFParser}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, FileInputStream, InputStream}

case class RdfFromJellyOptions(
    @ExtraName("input-file") inputFile: Option[String],
)

object RdfFromJelly extends JellyCommand[RdfFromJellyOptions]:
  override def group = "rdf"

  override def names: List[List[String]] = List(
    List("rdf", "from-jelly"),
  )

  override def run(options: RdfFromJellyOptions, remainingArgs: RemainingArgs): Unit =
    val inputStream = options.inputFile match {
      case Some(fileName: String) =>
        FileInputStream(File(fileName))
      case None => System.in
    }
    rewriteFormats(inputStream)

  /*
    This method reads the Jelly file, rewrite it to NQuads and writes it to the output stream
   * @param inputStream InputStream
   */
  private def rewriteFormats(inputStream: InputStream): Unit =
    // TODO: Add error handling; return success/failure in the future
    val interStream = new ByteArrayOutputStream()
    val interWriter = StreamRDFWriter.getWriterStream(interStream, RDFLanguages.NQUADS)
    RDFParser.source(inputStream).lang(JellyLanguage.JELLY).parse(interWriter)
    val readerStream = new ByteArrayInputStream(interStream.toByteArray)
    val nQuadWriter = StreamRDFWriter.getWriterStream(getOutputStream, RDFLanguages.NQUADS)
    RDFParser.source(readerStream).lang(RDFLanguages.NQUADS).parse(nQuadWriter)
