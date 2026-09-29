package eu.neverblink.jelly.cli.command.sparql

import caseapp.*
import com.google.protobuf.{InvalidProtocolBufferException, TextFormat}
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.sparql.util.SparqlFormat
import eu.neverblink.jelly.cli.util.io.ProtoText
import eu.neverblink.jelly.core.{RdfProtoDeserializationError, RdfProtoSerializationError}
import eu.neverblink.jelly.core.proto.v1.sparql.SparqlResultsFrame
import eu.neverblink.jelly.core.sparql.JellySparqlIoUtils
import org.apache.jena.riot.{RIOT, RiotException}
import org.apache.jena.riot.resultset.{ResultSetReaderRegistry, ResultSetWriterRegistry}

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

  /** Reads a result set in one format and writes it back out in another.
    *
    * Both SELECT results (bindings) and ASK results (a single boolean) are handled.
    */
  final def convert(
      from: SparqlFormat,
      to: SparqlFormat,
      inputStream: InputStream,
      outputStream: OutputStream,
  ): Unit =
    try {
      (from, to) match
        case (SparqlFormat.JellySparql, SparqlFormat.JellySparqlText) =>
          jellyBinaryToText(inputStream, outputStream)
        case (SparqlFormat.JellySparqlText, SparqlFormat.JellySparql) =>
          jellyTextToBinary(inputStream, outputStream)
        case (f: SparqlFormat.Jena, t: SparqlFormat.Jena) =>
          jenaConvert(f, t, inputStream, outputStream)
        case _ =>
          throw CriticalException(f"Conversion from $from to $to is not supported.")
      outputStream.flush()
    } catch
      // The Jelly RowSet reader wraps I/O errors (including protobuf ones) in a RiotException,
      // so unwrap it to report a malformed Jelly file the same way the rdf commands do.
      case e: RiotException =>
        e.getCause match
          case cause: InvalidProtocolBufferException => throw InvalidJellyFile(cause)
          case _ => throw JenaRiotException(e)
      case e: InvalidProtocolBufferException =>
        throw InvalidJellyFile(e)
      case e: RdfProtoDeserializationError =>
        throw JellyDeserializationError(e.getMessage)
      case e: RdfProtoSerializationError =>
        throw JellySerializationError(e.getMessage)
      case e: TextFormat.ParseException =>
        throw InvalidJellyFile(e)

  private def jenaConvert(
      from: SparqlFormat.Jena,
      to: SparqlFormat.Jena,
      inputStream: InputStream,
      outputStream: OutputStream,
  ): Unit =
    val context = RIOT.getContext.copy()
    val reader = ResultSetReaderRegistry.getFactory(from.jenaLang).create(from.jenaLang)
    val writer = ResultSetWriterRegistry.getFactory(to.jenaLang).create(to.jenaLang)
    val result = reader.readAny(inputStream, context)
    if result.isBoolean then
      writer.write(outputStream, result.getBooleanResult.booleanValue, context)
    else writer.write(outputStream, result.getResultSet, context)

  private val frameCommentPrefix = "# Frame"

  private def jellyBinaryToText(inputStream: InputStream, outputStream: OutputStream): Unit =
    val response = JellySparqlIoUtils.autodetectDelimiting(inputStream)
    val input = response.newInput()
    val frames =
      if response.isDelimited then
        Iterator.continually(SparqlResultsFrame.parseDelimitedFrom(input)).takeWhile(_ != null)
      else Iterator(SparqlResultsFrame.parseFrom(input))
    for (frame, i) <- frames.zipWithIndex do
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
    def writeFrame(text: String): Unit =
      ProtoText.parse(SparqlResultsFrame.getDescriptor, text).writeDelimitedTo(outputStream)

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
