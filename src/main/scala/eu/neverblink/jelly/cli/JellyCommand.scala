package eu.neverblink.jelly.cli

import caseapp.*
import eu.neverblink.jelly.cli.util.io.IoUtil

import java.io.*
import scala.compiletime.uninitialized

case class JellyCommandOptions(
    @HelpMessage("Add to run command in debug mode. ") debug: Boolean = false,
    @HelpMessage("Add to silence any warnings. ") quiet: Boolean = false,
)

trait HasJellyCommandOptions:
  @Recurse
  val common: JellyCommandOptions

abstract class JellyCommand[T <: HasJellyCommandOptions: {Parser, Help}] extends Command[T]:

  private var isTest = false
  private var options: Option[T] = None
  final protected[cli] var out = System.out
  final protected[cli] var err = System.err
  final protected[cli] var in = System.in

  private var osOut: ByteArrayOutputStream = uninitialized
  private var osErr: ByteArrayOutputStream = uninitialized

  /** Enable the "test mode" which captures stdout, stderr, exit code, and so on.
    * @param test
    *   true to enable, false to disable
    */
  private def testMode(test: Boolean): Unit =
    this.isTest = test
    if test then
      in = ByteArrayInputStream(Array())
      osOut = ByteArrayOutputStream()
      out = PrintStream(osOut)
      osErr = ByteArrayOutputStream()
      err = PrintStream(osErr)
    else
      in = System.in
      out = System.out
      err = System.err

  /** Check and set the values of all the general options repeating for every JellyCommand
    */
  private def setUpGeneralArgs(options: T): Unit =
    this.options = Some(options)

  /** Returns the options set up for this command
    */
  protected final def getOptions: T = options match {
    case Some(value) => value
    case None =>
      throw new CriticalException("Command tried to access options before they were set up")
  }

  /** Makes sure that the repetitive options needed for every JellyCommand are set up before calling
    * the doRun method, which contains Command-specific logic
    */
  final override def run(options: T, remainingArgs: RemainingArgs): Unit =
    setUpGeneralArgs(options)
    doRun(options, remainingArgs)

  /** This abstract method is the main entry point for every JellyCommand. It should be overridden
    * by Command-specific implementation, including logic needed for this specific object extendind
    * JellyCommand.
    */
  protected def doRun(options: T, remainingArgs: RemainingArgs): Unit

  /** Override to have custom error handling for Jelly commands
    */
  final override def main(progName: String, args: Array[String]): Unit =
    try super.main(progName, args)
    catch
      case e: Throwable =>
        ErrorHandler.handle(this, e)

  /** Returns information about whether the command is in debug mode (which returns stack traces of
    * every error) or not
    */
  final def isDebugMode: Boolean = this.getOptions.common.debug

  /** Returns information about whether the command is in quiet mode (which suppresses all warnings)
    * or not
    */
  final def isQuietMode: Boolean = this.getOptions.common.quiet

  /** Runs the command in test mode from the outside app parsing level
    * @param args
    *   the command line arguments
    */
  final def runTestCommand(args: List[String]): (String, String) =
    if !isTest then testMode(true)
    osOut.reset()
    osErr.reset()
    App.main(args.toArray)
    (osOut.toString("UTF-8"), osErr.toString("UTF-8"))

  private def validateTestMode(): Unit =
    if !isTest then throw new IllegalStateException("Not in test mode")

  final def getOutString: String =
    validateTestMode()
    out.flush()
    val s = osOut.toString
    osOut.reset()
    s

  final def getOutBytes: Array[Byte] =
    validateTestMode()
    out.flush()
    val b = osOut.toByteArray
    osOut.reset()
    b

  private final def getStdIn: InputStream =
    if isTest then in
    else System.in

  final def setStdIn(data: ByteArrayInputStream): Unit =
    in = data

  final def getOutStream: OutputStream =
    if isTest then osOut
    else System.out

  private def getStdOut: OutputStream =
    if isTest then osOut
    else System.out

  final def getErrString: String =
    validateTestMode()
    err.flush()
    val s = osErr.toString
    osErr.reset()
    s

  final def getErrBytes: Array[Byte] =
    validateTestMode()
    err.flush()
    val b = osErr.toByteArray
    osErr.reset()
    b

  /** This method matches the CLI input and output options to the correct file or standard
    * input/output
    * @param inputOption
    * @param outputOption
    * @return
    */
  final def getIoStreamsFromOptions(
      inputOption: Option[String],
      outputOption: Option[String],
  ): (InputStream, OutputStream) =
    val inputStream = inputOption match {
      case Some(fileName: String) =>
        IoUtil.inputStream(fileName)
      case _ => getStdIn
    }
    val outputStream = outputOption match {
      case Some(fileName: String) =>
        IoUtil.outputStream(fileName)
      case None => getStdOut
    }
    (inputStream, outputStream)

  @throws[ExitException]
  final def exit(code: Int, cause: Throwable): Nothing =
    if isTest then throw ExitException(code, Some(cause))
    else exit(code)

  @throws[ExitException]
  final override def exit(code: Int): Nothing =
    if isTest then throw ExitException(code)
    else super.exit(code)

  final override def printLine(line: String, toStderr: Boolean): Unit =
    if toStderr then err.println(line)
    else out.println(line)
