package eu.neverblink.jelly.cli

import caseapp.*

import java.io.{ByteArrayOutputStream, OutputStream, PrintStream}
import scala.compiletime.uninitialized

case class JellyOptions(
    @HelpMessage("Add to run command in debug mode") debug: Boolean = false,
)

trait HasJellyOptions:
  @Recurse
  val common: JellyOptions

abstract class JellyCommand[T <: HasJellyOptions: {Parser, Help}] extends Command[T]:
  private var isTest = false
  private var isDebug = false
  protected[cli] var out = System.out
  protected[cli] var err = System.err
  private var osOut: ByteArrayOutputStream = uninitialized
  private var osErr: ByteArrayOutputStream = uninitialized

  /** Enable the "test mode" which captures stdout, stderr, exit code, and so on.
    * @param test
    *   true to enable, false to disable
    */
  def testMode(test: Boolean): Unit =
    this.isTest = test
    if test then
      osOut = ByteArrayOutputStream()
      out = PrintStream(osOut)
      osErr = ByteArrayOutputStream()
      err = PrintStream(osErr)
    else
      out = System.out
      err = System.err

  /** Check and set the values of all the general options repeating for every JellyCommand
    */
  private def setUpGeneralArgs(options: T, remainingArgs: RemainingArgs): Unit =
    if options.common.debug then this.isDebug = true

  /** Makes sure that the repetitive options needed for every JellyCommand are set up before calling
    * the doRun method, which contains Command-specific logic
    */
  final override def run(options: T, remainingArgs: RemainingArgs): Unit =
    setUpGeneralArgs(options, remainingArgs)
    doRun(options, remainingArgs)

  /** This abstract method is the main entry point for every JellyCommand. It should be overridden
    * by Command-specific implementation, including logic needed for this specific object extendind
    * JellyCommand.
    */
  def doRun(options: T, remainingArgs: RemainingArgs): Unit

  /** Override to have custom error handling for Jelly commands
    */
  override def main(progName: String, args: Array[String]): Unit =
    try super.main(progName, args)
    catch
      case e: Throwable =>
        ErrorHandler.handle(this, e)

  /** Returns information about whether the command is in debug mode (which returns stack traces of
    * every error) or not
    */
  def isDebugMode: Boolean = this.isDebug

  /** Runs the command in test mode from the outside app parsing level
    * @param args
    *   the command line arguments
    */
  def runTestCommand(args: List[String]): (String, String) =
    if !isTest then testMode(true)
    osOut.reset()
    osErr.reset()
    App.main(args.toArray)
    (osOut.toString, osErr.toString)

  def getOutContent: String =
    if isTest then
      out.flush()
      val s = osOut.toString
      osOut.reset()
      s
    else throw new IllegalStateException("Not in test mode")

  def getOutStream: OutputStream =
    if isTest then osOut
    else System.out

  protected def getStdOut: OutputStream =
    if isTest then osOut
    else System.out

  def getErrContent: String =
    if isTest then
      err.flush()
      val s = osErr.toString
      osErr.reset()
      s
    else throw new IllegalStateException("Not in test mode")

  @throws[ExitException]
  override def exit(code: Int): Nothing =
    if isTest then throw ExitException(code)
    else super.exit(code)

  override def printLine(line: String, toStderr: Boolean): Unit =
    if toStderr then err.println(line)
    else out.println(line)
