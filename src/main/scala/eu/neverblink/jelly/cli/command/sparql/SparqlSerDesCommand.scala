package eu.neverblink.jelly.cli.command.sparql

import caseapp.*
import com.google.protobuf.InvalidProtocolBufferException
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.sparql.util.SparqlFormat
import eu.neverblink.jelly.core.{RdfProtoDeserializationError, RdfProtoSerializationError}
import org.apache.jena.riot.{RIOT, RiotException}
import org.apache.jena.riot.resultset.{ResultSetReaderRegistry, ResultSetWriterRegistry}

import java.io.{InputStream, OutputStream}

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
      val context = RIOT.getContext.copy()
      val reader = ResultSetReaderRegistry.getFactory(from.jenaLang).create(from.jenaLang)
      val writer = ResultSetWriterRegistry.getFactory(to.jenaLang).create(to.jenaLang)
      val result = reader.readAny(inputStream, context)
      if result.isBoolean then
        writer.write(outputStream, result.getBooleanResult.booleanValue, context)
      else writer.write(outputStream, result.getResultSet, context)
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
