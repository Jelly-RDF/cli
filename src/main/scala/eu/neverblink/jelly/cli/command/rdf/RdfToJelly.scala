package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.RdfFormat.*
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{Lang, RDFParser}

import java.io.{InputStream, OutputStream}

object RdfToJellyPrint extends RdfCommandPrintUtil[RdfFormat.Jena.Readable]:
  override val defaultFormat: RdfFormat = RdfFormat.NQuads

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

object RdfToJelly extends RdfCommand[RdfToJellyOptions, RdfFormat.Jena.Readable]:

  override def names: List[List[String]] = List(
    List("rdf", "to-jelly"),
  )

  lazy val printUtil: RdfCommandPrintUtil[RdfFormat.Jena.Readable] = RdfToJellyPrint

  val defaultAction: (InputStream, OutputStream) => Unit =
    langToJelly(RdfFormat.NQuads.jenaLang, _, _)

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
      option: RdfFormat.Jena.Readable,
  ): Option[(InputStream, OutputStream) => Unit] =
    Some(langToJelly(option.jenaLang, _, _))

  /** This method reads the file, rewrites it to Jelly and writes it to some output stream
    * @param jenaLang
    *   Language that should be converted to Jelly
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    */
  private def langToJelly(
      jenaLang: Lang,
      inputStream: InputStream,
      outputStream: OutputStream,
  ): Unit =
    val jellyWriter = StreamRDFWriter.getWriterStream(outputStream, JellyLanguage.JELLY)
    RDFParser.source(inputStream).lang(jenaLang).parse(jellyWriter)
