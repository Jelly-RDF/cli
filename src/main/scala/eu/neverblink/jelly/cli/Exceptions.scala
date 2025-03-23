package eu.neverblink.jelly.cli

import com.google.protobuf.InvalidProtocolBufferException
import org.apache.jena.riot.RiotException

/** Contains a set of common jelly-cli exceptions with custom output messages.
  */

case class InputFileNotFound(file: String)
    extends CriticalException(s"Input file $file does not exist.")
case class InputFileInaccessible(file: String)
    extends CriticalException(s"Not enough permissions to read input file $file.")
case class OutputFileCannotBeCreated(file: String)
    extends CriticalException(
      s"Not enough permissions to create output file $file in this directory.",
    )
case class JellyDeserializationError(message: String)
    extends CriticalException(s"Jelly deserialization error: $message")
case class JellySerializationError(message: String)
    extends CriticalException(s"Jelly serialization error: $message")
case class JellyTranscodingError(message: String)
    extends CriticalException(s"Jelly transcoding error: $message")
case class JenaRiotException(e: RiotException)
    extends CriticalException(s"Riot exception: ${e.getMessage}")
case class InvalidJellyFile(e: InvalidProtocolBufferException)
    extends CriticalException(s"Invalid Jelly file: ${e.getMessage}")
case class ExitException(code: Int) extends CriticalException(s"Exiting with code $code.")

class CriticalException(message: String) extends Exception(message)
