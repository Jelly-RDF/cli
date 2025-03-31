package eu.neverblink.jelly.cli.command.rdf

import com.google.protobuf.InvalidProtocolBufferException
import org.apache.jena.riot.RiotException
import eu.neverblink.jelly.cli.*
import caseapp.*
import eu.ostrzyciel.jelly.core.{RdfProtoSerializationError, RdfProtoDeserializationError}

import java.io.{InputStream, OutputStream}

/** This abstract class is responsible for the common logic in both RDF parsing commands
  */
abstract class RdfCommand[T <: HasJellyOptions: {Parser, Help}] extends JellyCommand[T]:

  override final def group = "rdf"

  /** What is the default action if no formats specified */
  val defaultAction: (InputStream, OutputStream) => Unit

  /** The print util responsible for handling the specific formats etc the command requires */
  lazy val printUtil: RdfCommandPrintUtil

  /** The method responsible for matching the format to a given action */
  def matchToAction[R <: RdfFormat](option: R): Option[(InputStream, OutputStream) => Unit]

  /** This method takes care of proper error handling and takes care of the parameter priorities in
    * matching the input to a given format conversion
    *
    * @param inputStream
    *   InputStream
    * @param outputStream
    *   OutputStream
    * @param format
    *   Option[String]
    * @param fileName
    *   Option[String]
    * @throws JellyDeserializationError
    * @throws JenaRiotException
    * @throws InvalidJellyFile
    */
  def parseFormatArgs(
      inputStream: InputStream,
      outputStream: OutputStream,
      format: Option[String],
      fileName: Option[String],
  ): Unit =
    type ValidFormat = RdfFormat.Jena.Writeable | RdfFormat.JellyText.type
    RdfFormat.all.collect { case x: ValidFormat => x }

    try {
      val explicitFormat = if (format.isDefined) RdfFormat.find(format.get) else None
      val implicitFormat =
        if (fileName.isDefined) RdfFormat.inferFormat(fileName.get) else None
      (explicitFormat, implicitFormat) match {
        case (Some(f: RdfFormat), _) if matchToAction(f).isDefined =>
          matchToAction(f).get(inputStream, outputStream)
        // If format explicitely defined but does not match any available actions or formats, we throw an error
        case (_, _) if format.isDefined =>
          throw InvalidFormatSpecified(format.get, printUtil.validFormatsString)
        case (_, Some(f: RdfFormat)) if matchToAction(f).isDefined =>
          matchToAction(f).get(inputStream, outputStream)
        // If format not explicitely defined but implicitely not understandable we default to this
        case (_, _) => defaultAction(inputStream, outputStream)
      }
    } catch
      case e: RiotException =>
        throw JenaRiotException(e)
      case e: InvalidProtocolBufferException =>
        throw InvalidJellyFile(e)
      case e: RdfProtoDeserializationError =>
        throw JellyDeserializationError(e.getMessage)
      case e: RdfProtoSerializationError =>
        throw JellySerializationError(e.getMessage)
