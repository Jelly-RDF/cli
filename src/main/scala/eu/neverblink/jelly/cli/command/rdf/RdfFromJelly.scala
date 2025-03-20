package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import eu.neverblink.jelly.cli.JellyCommand
import eu.ostrzyciel.jelly.convert.jena.riot.{JellyLanguage, JellySubsystemLifecycle}
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{RDFLanguages, RDFParser}

import java.io.{File, FileInputStream, FileOutputStream, InputStream, OutputStream}

case class RdfFromJellyOptions(
    @ExtraName("to") outputFile: Option[String] = None,
)

object RdfFromJelly extends JellyCommand[RdfFromJellyOptions]:
  override def group = "rdf"

  override def names: List[List[String]] = List(
    List("rdf", "from-jelly"),
  )

  override def run(options: RdfFromJellyOptions, remainingArgs: RemainingArgs): Unit =
    val inputStream = remainingArgs.remaining.headOption match {
      case Some(fileName: String) =>
        FileInputStream(File(fileName))
      case _ => System.in
    }
    val outputStream = options.outputFile match {
      case Some(fileName: String) =>
        FileOutputStream(fileName)
      case None => getStdOut
    }
    doConversion(inputStream, outputStream)

  /*
    This method reads the Jelly file, rewrite it to NQuads and writes it to some output stream
   * @param inputStream InputStream
   * @param outputStream OutputStream
   */
  private def doConversion(inputStream: InputStream, outputStream: OutputStream): Unit =
    val mod = JellySubsystemLifecycle()
    mod.start()
    val nQuadWriter = StreamRDFWriter.getWriterStream(outputStream, RDFLanguages.NQUADS)
    RDFParser.source(inputStream).lang(JellyLanguage.JELLY).parse(nQuadWriter)
