package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.RdfFormat.*
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.riot.system.StreamRDFWriter
import org.apache.jena.riot.{Lang, RDFParser, RIOT}

import java.io.{InputStream, OutputStream}

object RdfToJellyPrint extends RdfCommandPrintUtil[RdfFormat.Jena.Readable]:
  override val defaultFormat: RdfFormat = RdfFormat.NQuads

case class RdfToJellyOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @ExtraName("to") outputFile: Option[String] = None,
    @ValueDescription("Input format.")
    @HelpMessage(
      RdfToJellyPrint.helpMsg,
    )
    @ExtraName("in-format") inputFormat: Option[String] = None,
    @Recurse
    jellySerializationOptions: RdfJellySerializationOptions = RdfJellySerializationOptions(),
    @HelpMessage(
      "Target number of rows per frame – the writer may slightly exceed that. Default: 256",
    )
    rowsPerFrame: Int = 256,
    @HelpMessage(
      "Whether to preserve explicit namespace declarations in the output (PREFIX: in Turtle). " +
        "Default: false",
    )
    enableNamespaceDeclarations: Boolean = false,
    @HelpMessage(
      "Whether the output should be delimited. Setting it to false will force the output to be a single " +
        "frame – make sure you know what you are doing. Default: true",
    )
    delimited: Boolean = true,
) extends HasJellyCommandOptions

object RdfToJelly extends RdfCommand[RdfToJellyOptions, RdfFormat.Jena.Readable]:

  override def names: List[List[String]] = List(
    List("rdf", "to-jelly"),
  )

  lazy val printUtil: RdfCommandPrintUtil[RdfFormat.Jena.Readable] = RdfToJellyPrint

  val defaultAction: (InputStream, OutputStream) => Unit =
    langToJelly(RdfFormat.NQuads.jenaLang, _, _)

  override def doRun(options: RdfToJellyOptions, remainingArgs: RemainingArgs): Unit =
    // Touch the options to make sure they are valid
    options.jellySerializationOptions.asRdfStreamOptions
    val (inputStream, outputStream) =
      getIoStreamsFromOptions(remainingArgs.remaining.headOption, options.outputFile)
    parseFormatArgs(
      inputStream,
      outputStream,
      options.inputFormat,
      remainingArgs.remaining.headOption,
    )

  override def matchFormatToAction(
      format: RdfFormat.Jena.Readable,
  ): Option[(InputStream, OutputStream) => Unit] =
    Some(langToJelly(format.jenaLang, _, _))

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
    // Configure the writer
    val writerContext = RIOT.getContext.copy()
      .set(
        JellyLanguage.SYMBOL_STREAM_OPTIONS,
        getOptions.jellySerializationOptions.asRdfStreamOptions,
      )
      .set(JellyLanguage.SYMBOL_FRAME_SIZE, getOptions.rowsPerFrame)
      .set(
        JellyLanguage.SYMBOL_ENABLE_NAMESPACE_DECLARATIONS,
        getOptions.enableNamespaceDeclarations,
      )
      .set(JellyLanguage.SYMBOL_DELIMITED_OUTPUT, getOptions.delimited)
    val jellyWriter = StreamRDFWriter.getWriterStream(
      outputStream,
      JellyLanguage.JELLY,
      writerContext,
    )
    RDFParser.source(inputStream).lang(jenaLang).parse(jellyWriter)
