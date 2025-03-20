package eu.neverblink.jelly.cli.command.helpers
import eu.neverblink.jelly.cli.{App, JellyCommand}

object TestAppRunner:
  /*
   * Runs a command in test mode from the outside app parsing level
   * @param command
   *  the command to run
   * @param args
   * the command line arguments
   */
  def runCommand(command: JellyCommand[?], args: List[String]): (String, String) =
    command.setUpTest()
    App.main(args.toArray)
    (command.getOut, command.getErr)
