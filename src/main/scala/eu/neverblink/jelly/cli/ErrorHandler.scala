package eu.neverblink.jelly.cli

import java.io.FileNotFoundException

case class ExitException(code: Int) extends Exception

type CriticalExceptions = IllegalArgumentException | ExitException | FileNotFoundException
type NonCriticalExceptions = IllegalStateException | ExitException

/*
 * Handle exceptions. Critical exceptions will exit the application with code 1.
 * Non-critical exceptions will be printed to std err.
 */

object ErrorHandler:

  def handle(command: JellyCommand[?], t: Throwable): Unit =
    t match
      case e: CriticalExceptions =>
        System.err.println(s"Critical error: ${e.getMessage}")
        command.exit(1)
      case e: NonCriticalExceptions =>
        System.err.println(s"Non-critical error: ${e.getMessage}")
      case _ => throw t

  /*
   * Add pretty descriptions for most common exceptions
   */
  def describeMostCommon(t: Throwable): String =
    t match
      case e: IllegalArgumentException => "Illegal argument"
      case e: IllegalStateException => "Illegal state"
      case e: ExitException => "Exit"
      case e: FileNotFoundException => "File not found"
      case _ => "Unknown"
