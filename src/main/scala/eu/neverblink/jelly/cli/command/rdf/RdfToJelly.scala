package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.RdfFormatOption.*
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{RDFLanguages, RDFParser}

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

object RdfToJelly extends RdfCommand[RdfToJellyOptions]:

  override def names: List[List[String]] = List(
    List("rdf", "to-jelly"),
  )

  lazy val printUtil: RdfCommandPrintUtil = RdfToJellyPrint

  def defaultAction: (InputStream, OutputStream) => Unit = nQuadToJelly

  override def doRun(options: RdfToJellyOptions, remainingArgs: RemainingArgs): Unit =
    val (inputStream, outputStream) =
      getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    parseFormatArgs(
      inputStream,
      outputStream,
      options.inputFormat,
      remainingArgs.remaining.headOption,
    )

  override def matchToAction(
      option: RdfFormatOption,
  ): Option[(InputStream, OutputStream) => Unit] =
    option match
      case NQuads => Some(nQuadToJelly)
      case _ => None

  /** This method reads the NQuad file, rewrites it to Jelly and writes it to some output stream
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    */
  private def nQuadToJelly(inputStream: InputStream, outputStream: OutputStream): Unit =
    val jellyWriter = StreamRDFWriter.getWriterStream(outputStream, JellyLanguage.JELLY)
    RDFParser.source(inputStream).lang(RDFLanguages.NQUADS).parse(jellyWriter)
