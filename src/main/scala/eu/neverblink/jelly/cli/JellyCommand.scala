package eu.neverblink.jelly.cli

import caseapp.*

import java.io.{ByteArrayOutputStream, OutputStream, PrintStream}
import scala.compiletime.uninitialized

abstract class JellyCommand[T: {Parser, Help}] extends Command[T]:
  private var isTest = false
  private var out = System.out
  private var err = System.err
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

  /** Runs the command in test mode from the outside app parsing level
    * @param args
    *   the command line arguments
    */
  def runCommand(args: List[String]): (String, String) =
    if !isTest then testMode(true)
    osOut.reset()
    osErr.reset()
    App.main(args.toArray)
    (osOut.toString, osErr.toString)

  def getOut: String =
    if isTest then
      out.flush()
      val s = osOut.toString
      osOut.reset()
      s
    else throw new IllegalStateException("Not in test mode")

  protected def getStdOut: OutputStream =
    if isTest then osOut
    else System.out

  def getErr: String =
    if isTest then
      err.flush()
      val s = osErr.toString
      osErr.reset()
      s
    else throw new IllegalStateException("Not in test mode")

  @throws[ExitError]
  override def exit(code: Int): Nothing =
    if isTest then throw ExitError(code)
    else super.exit(code)

  override def printLine(line: String, toStderr: Boolean): Unit =
    if toStderr then err.println(line)
    else out.println(line)
