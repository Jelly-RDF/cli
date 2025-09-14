package eu.neverblink.jelly.cli

/** Handle exceptions. Common critical exceptions will be given custom output messages.
  */

object ErrorHandler:

  def handle(command: JellyCommand[?], t: Throwable): Unit =
    t match
      case e: CriticalException =>
        command.printLine(f"${e.getMessage}", toStderr = true)
        printStackTraceIfDebug(command, t)
      case e: Throwable =>
        command.printLine("Unknown error", toStderr = true)
        // Always print stack trace for unknown exceptions,
        // as otherwise the user has no clue what happened.
        t.printStackTrace(command.err)
    command.exit(1, t)

  /** Print out stack trace or debugging information
    * @param command
    * @param t
    */
  private def printStackTraceIfDebug(
      command: JellyCommand[?],
      t: Throwable,
  ): Unit =
    if command.isDebugMode then t.printStackTrace(command.err)
    else command.printLine("Run with --debug to see the complete stack trace.", toStderr = true)
