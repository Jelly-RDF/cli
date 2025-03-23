package eu.neverblink.jelly.cli

/** Handle exceptions. Common critical exceptions will be given custom output messages.
  */

object ErrorHandler:

  def handle(command: JellyCommand[?], t: Throwable): Unit =
    t match
      case e: CriticalException =>
        command.printLine(f"${e.getMessage}", toStderr = true)
      case e: Throwable =>
        command.printLine("Unknown error", toStderr = true)
    printStackTrace(command, t)
    command.exit(1)

  /** Print out stack trace or debugging information
    * @param command
    * @param t
    */
  private def printStackTrace(
      command: JellyCommand[?],
      t: Throwable,
  ): Unit =
    if command.isDebugMode then t.printStackTrace(command.err)
    else command.printLine("Run with --debug to see the complete stack trace.", toStderr = true)
