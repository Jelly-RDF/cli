package eu.neverblink.jelly.cli.command.sparql

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.sparql.util.{
  SparqlFormat,
  SparqlJellyUtil,
  SparqlResultSetReader,
}
import eu.neverblink.jelly.cli.util.io.ProtoText
import eu.neverblink.jelly.convert.jena.sparql.{
  JellySparqlLanguage,
  JenaSparqlConverterFactory,
  RowSetReaderJelly,
  RowSetWriterJelly,
}
import eu.neverblink.jelly.core.proto.v1.sparql.{
  SparqlResultsFrame,
  SparqlResultsOptions,
  SparqlStreamType,
}
import org.apache.jena.riot.RIOT
import org.apache.jena.riot.rowset.{RowSetReaderRegistry, RowSetWriterRegistry}
import org.apache.jena.sparql.exec.QueryExecResult
import org.apache.jena.sparql.util.Context

import java.io.{BufferedReader, InputStream, InputStreamReader, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Using

/** Common logic for the two SPARQL result set conversion commands.
  */
abstract class SparqlSerDesCommand[T <: HasJellyCommandOptions: {Parser, Help}]
    extends JellyCommand[T]:

  override final def group = "sparql"

  /** Formats the user can pick from for the non-Jelly side of the conversion. */
  val validFormats: List[SparqlFormat]

  /** Format assumed when the user gives neither an explicit format nor a recognizable file name. */
  val defaultFormat: SparqlFormat

  /** Picks the non-Jelly format.
    *
    * @throws InvalidFormatSpecified
    *   if the user asked for a format this command cannot handle
    */
  final def resolveFormat(format: Option[String], fileName: Option[String]): SparqlFormat =
    format match
      case Some(name) =>
        SparqlFormat.find(name).filter(validFormats.contains).getOrElse {
          throw InvalidFormatSpecified(name, SparqlFormat.validFormatsString(validFormats))
        }
      case None =>
        fileName
          .flatMap(SparqlFormat.inferFormat)
          .filter(validFormats.contains)
          .getOrElse(defaultFormat)

  /** Context passed to Jena's result set reader and writer. Commands override this to configure the
    * Jelly-SPARQL writer.
    */
  protected def jenaContext: Context = RIOT.getContext.copy()

  /** Whether the Jelly-SPARQL output should be delimited. */
  protected def delimitedOutput: Boolean = true

  /** Stream options of the Jelly-SPARQL output, made from the options of the Jelly-SPARQL input.
    * Commands that let the user set the options override this.
    */
  protected def jellySparqlOutputOptions(inputOptions: SparqlResultsOptions): SparqlResultsOptions =
    SparqlJellyUtil.defaultOptions(inputOptions)

  /** Converts Jelly-RDF to Jelly-SPARQL. Commands that read Jelly-RDF override this. */
  protected def jellyRdfToSparql(inputStream: InputStream, outputStream: OutputStream): Unit =
    throw CriticalException("This command cannot read Jelly-RDF.")

  /** Converts Jelly-SPARQL to Jelly-RDF. Commands that write Jelly-RDF override this. */
  protected def jellySparqlToRdf(inputStream: InputStream, outputStream: OutputStream): Unit =
    throw CriticalException("This command cannot write Jelly-RDF.")

  /** Reads a result set in one format and writes it back out in another.
    *
    * Both SELECT results (bindings) and ASK results (a single boolean) are handled. All result sets
    * of a PUNCTUATED stream are written to the same output, one after another.
    */
  final def convert(
      from: SparqlFormat,
      to: SparqlFormat,
      inputStream: InputStream,
      outputStream: OutputStream,
  ): Unit =
    SparqlJellyUtil.translateErrors {
      (from, to) match
        case (SparqlFormat.JellySparql, SparqlFormat.JellySparqlText) =>
          jellyBinaryToText(inputStream, outputStream)
        case (SparqlFormat.JellySparqlText, SparqlFormat.JellySparql) =>
          jellyTextToBinary(inputStream, outputStream)
        case (SparqlFormat.JellyRdf, SparqlFormat.JellySparql) =>
          jellyRdfToSparql(inputStream, outputStream)
        case (SparqlFormat.JellySparql, SparqlFormat.JellyRdf) =>
          jellySparqlToRdf(inputStream, outputStream)
        case (SparqlFormat.JellySparql, SparqlFormat.JellySparql) =>
          jellySparqlToSparql(inputStream, outputStream)
        case (f: SparqlFormat.Jena, t: SparqlFormat.Jena) =>
          jenaConvert(f, t, inputStream, outputStream)
        case _ =>
          throw CriticalException(f"Conversion from $from to $to is not supported.")
      outputStream.flush()
    }

  private def jenaConvert(
      from: SparqlFormat.Jena,
      to: SparqlFormat.Jena,
      inputStream: InputStream,
      outputStream: OutputStream,
  ): Unit =
    val context = jenaContext
    // The result sets of a PUNCTUATED stream are written one after another
    readResults(from, inputStream, context)(write(to, outputStream, context, _))

  /** Reads the result sets of the input, in order. A Jelly-SPARQL stream may have many of them,
    * other formats have one.
    */
  protected final def readResults(
      from: SparqlFormat.Jena,
      inputStream: InputStream,
      context: Context,
  )(consumer: QueryExecResult => Unit): Unit =
    from match
      case SparqlFormat.JellySparql =>
        RowSetReaderJelly(RowSetReaderJelly.Options(), JenaSparqlConverterFactory.getInstance())
          .readAll(inputStream, context, consumer(_))
      case _ =>
        consumer(
          RowSetReaderRegistry.getFactory(from.jenaLang).create(from.jenaLang)
            .readAny(inputStream, context),
        )

  /** Writes one result set, as a whole stream in the given format. */
  private def write(
      to: SparqlFormat.Jena,
      outputStream: OutputStream,
      context: Context,
      result: QueryExecResult,
  ): Unit =
    val writer = RowSetWriterRegistry.getFactory(to.jenaLang).create(to.jenaLang)
    if result.isBoolean then writer.write(outputStream, result.booleanResult.booleanValue, context)
    else writer.write(outputStream, result.rowSet, context)

  /** Starts a PUNCTUATED Jelly-SPARQL stream, with the options in the context. */
  protected final def resultSetsWriter(
      outputStream: OutputStream,
      context: Context,
  ): RowSetWriterJelly.ResultSetsWriter =
    RowSetWriterJelly(RowSetWriterJelly.Options(), JenaSparqlConverterFactory.getInstance())
      .resultSetsWriter(outputStream, context)

  /** Writes the result set as the next one of a PUNCTUATED stream. */
  protected final def writeNext(
      writer: RowSetWriterJelly.ResultSetsWriter,
      result: QueryExecResult,
  ): Unit =
    if result.isBoolean then writer.write(result.booleanResult.booleanValue)
    else writer.write(result.rowSet)

  /** Re-encodes a Jelly-SPARQL stream, with the options from [[jellySparqlOutputOptions]].
    *
    * All result sets of a PUNCTUATED output go into one stream. A FLAT output can only have one.
    */
  private def jellySparqlToSparql(inputStream: InputStream, outputStream: OutputStream): Unit =
    val resultSets = SparqlResultSetReader(SparqlJellyUtil.iterateSparqlStream(inputStream))
    if !resultSets.hasNext then throw CriticalException("Empty input stream")
    val options = jellySparqlOutputOptions(resultSets.options)
    val context = jenaContext.set(JellySparqlLanguage.SYMBOL_STREAM_OPTIONS, options)
    if options.getStreamType == SparqlStreamType.PUNCTUATED then
      if !delimitedOutput then
        throw InvalidArgument(
          "--delimited",
          "false",
          Some("PUNCTUATED streams are always written delimited"),
        )
      val writer = resultSetsWriter(outputStream, context)
      resultSets.foreach(r => writeNext(writer, r.toQueryExecResult))
    else
      write(SparqlFormat.JellySparql, outputStream, context, resultSets.next().toQueryExecResult)
      if resultSets.hasNext then
        throw CriticalException(
          "The input has more than one result set, so it can only be written as a PUNCTUATED " +
            "stream.",
        )

  private val frameCommentPrefix = "# Frame"

  private def jellyBinaryToText(inputStream: InputStream, outputStream: OutputStream): Unit =
    for (frame, i) <- SparqlJellyUtil.iterateSparqlStream(inputStream).zipWithIndex do
      outputStream.write(f"$frameCommentPrefix $i\n".getBytes(UTF_8))
      val text = ProtoText.print(SparqlResultsFrame.getDescriptor, frame.toByteArray)
      outputStream.write(text.getBytes(UTF_8))

  /** Frames are split on the "# Frame" comments. Without them, the whole input is one frame. */
  private def jellyTextToBinary(inputStream: InputStream, outputStream: OutputStream): Unit =
    if !isQuietMode then
      printLine(
        "WARNING: The Jelly-SPARQL text format is not stable and may change in incompatible " +
          "ways in the future.\nIt's only intended for testing and development.\n" +
          "NEVER use it in production.\nUse --quiet to silence this warning.",
        true,
      )
    var frameCount = 0
    def writeFrame(text: String): Unit =
      frameCount += 1
      val frame = ProtoText.parse(SparqlResultsFrame.getDescriptor, text)
      if delimitedOutput then frame.writeDelimitedTo(outputStream)
      else if frameCount == 1 then frame.writeTo(outputStream)
      // Concatenated non-delimited frames would be read back as one frame with merged columns
      else
        throw CriticalException(
          "The input has more than one frame, so it cannot be written as non-delimited output.",
        )

    Using.resource(BufferedReader(InputStreamReader(inputStream, UTF_8))) { reader =>
      val buffer = StringBuilder()
      var hasContent = false
      Iterator.continually(reader.readLine()).takeWhile(_ != null).foreach { line =>
        if line.startsWith(frameCommentPrefix) then
          if hasContent then writeFrame(buffer.toString)
          buffer.clear()
          hasContent = false
        else
          buffer.append(line).append('\n')
          val trimmed = line.trim
          if trimmed.nonEmpty && !trimmed.startsWith("#") then hasContent = true
      }
      if hasContent then writeFrame(buffer.toString)
    }
