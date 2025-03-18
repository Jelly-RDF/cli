package eu.neverblink.jelly.cli.command

import caseapp.*

case class FoolAroundOptions(
  @HelpMessage("What to say")
  say: String = "Hello, World!"
)

/**
 * "foo-bar" kind of command.
 */
object FoolAround extends Command[FoolAroundOptions]:
  // https://alexarchambault.github.io/case-app/commands/
  override def names: List[List[String]] = List(
    List("fool-around"),
  )

  override def run(options: FoolAroundOptions, remainingArgs: RemainingArgs): Unit =
    println(options.say)
