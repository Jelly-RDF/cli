package eu.neverblink.jelly.cli.command.sparql.util

import com.google.protobuf.{InvalidProtocolBufferException, TextFormat}
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.util.io.IoUtil
import eu.neverblink.jelly.core.{RdfProtoDeserializationError, RdfProtoSerializationError}
import eu.neverblink.jelly.core.proto.v1.sparql.{SparqlResultsFrame, SparqlResultsOptions}
import eu.neverblink.jelly.core.sparql.JellySparqlIoUtils
import org.apache.jena.riot.RiotException

import java.io.InputStream
import scala.util.Using

/** The Jelly-SPARQL counterpart of [[eu.neverblink.jelly.cli.command.rdf.util.JellyUtil]]. */
object SparqlJellyUtil:

  /** Reads a Jelly-SPARQL stream and returns an iterator of its frames.
    */
  def iterateSparqlStream(inputStream: InputStream): Iterator[SparqlResultsFrame] =
    iterateSparqlStreamWithDelimitingInfo(inputStream)._2

  /** Reads a Jelly-SPARQL stream and returns an iterator of its frames and a boolean saying whether
    * the stream is delimited.
    */
  def iterateSparqlStreamWithDelimitingInfo(
      inputStream: InputStream,
  ): (Boolean, Iterator[SparqlResultsFrame]) =
    val response = JellySparqlIoUtils.autodetectDelimiting(inputStream)
    val input = response.newInput()
    if response.isDelimited then
      (
        true,
        Iterator.continually(SparqlResultsFrame.parseDelimitedFrom(input)).takeWhile(_ != null),
      )
    else
      val frame = SparqlResultsFrame.parseFrom(input)
      // An empty input parses as an empty frame, but it's really an empty stream
      (false, if frame.getSerializedSize == 0 then Iterator.empty else Iterator(frame))

  /** Reads the stream options from the first frame of a Jelly-SPARQL file.
    *
    * @throws CriticalException
    *   if the file is empty or its first frame has no options
    */
  def loadOptionsFromFile(fileName: String): SparqlResultsOptions =
    translateErrors {
      Using.resource(IoUtil.inputStream(fileName)) { is =>
        val frames = iterateSparqlStream(is)
        if !frames.hasNext then throw CriticalException(s"The file $fileName is empty")
        val options = frames.next().getOptions
        if options == null then
          throw CriticalException(
            s"The first frame in $fileName does not contain stream options",
          )
        options
      }
    }

  /** Runs `body` and turns the errors of Jelly, protobuf, and Jena into the CLI's own exceptions.
    */
  def translateErrors[T](body: => T): T =
    try body
    catch
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
