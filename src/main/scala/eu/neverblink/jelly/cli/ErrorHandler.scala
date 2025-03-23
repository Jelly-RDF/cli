package eu.neverblink.jelly.cli

case class InputFileNotFound(file: String)
    extends CriticalException(s"Input file $file does not exist.")
case class InputFileInaccessible(file: String)
    extends CriticalException(s"Not enough permissions to read input file $file.")
case class OutputFileCannotBeCreated(file: String)
    extends CriticalException(
      s"Not enough permissions to create output file $file in this directory.",
    )
case class JellyDeserializationError(message: String)
    extends ParsingError(s"Jelly deserialization error: $message.")
case class JellySerializationError(message: String)
    extends ParsingError(s"Jelly serialization error: $message.")
case class JellyTranscodingError(message: String)
    extends ParsingError(s"Jelly transcoding error: $message.")
class ParsingError(message: String) extends CriticalException(s"Parsing error: $message.")
case class InputOutputTranslationLossy(format: String)
    extends NonCriticalException(
      "Input file cannot be fully translated to output format. The translation will be lossy.",
    )
case class ExitException(code: Int) extends CriticalException(s"Exiting with code $code.")

class CriticalException(message: String) extends Exception(message)
class NonCriticalException(message: String) extends Exception(message)

/** Handle exceptions. Critical exceptions will exit the application with code 1. Non-critical
  * exceptions will be printed to std err.
  */

object ErrorHandler:

  def handle(command: JellyCommand[?], t: Throwable): Unit =
    t match
      case e: ParsingError =>
        command.printLine(f"${e.getMessage}", toStderr = true)
        printStackTrace(command, e)
        command.exit(1)
      case e: CriticalException =>
        command.printLine(f"${e.getMessage}", toStderr = true)
        command.exit(1)
      case e: NonCriticalException =>
        command.printLine(f"${e.getMessage}", toStderr = true)
      case e: Throwable =>
        command.printLine("Unknown error", toStderr = true)
        printStackTrace(command, e)
        command.exit(1)

  /** Print out stack trace or debugging information
    * @param command
    * @param t
    */
  private def printStackTrace(
      command: JellyCommand[?],
      t: Throwable,
  ): Unit =
    if App.debugMode then t.printStackTrace(command.err)
    else command.printLine("If needed, run with --debug to see the stack trace.", toStderr = true)
