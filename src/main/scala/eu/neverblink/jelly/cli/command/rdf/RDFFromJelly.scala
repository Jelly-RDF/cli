package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import eu.neverblink.jelly.cli.JellyCommand

case class FromJellyRDFOptions()

object RDFFromJelly extends JellyCommand[FromJellyRDFOptions]:
  override def names: List[List[String]] = List(
    List("rdf from-jelly"),
  )

  override def run(options: FromJellyRDFOptions, remainingArgs: RemainingArgs): Unit =
    // This is a placeholder for the actual implementation
    println("rdf from-jelly")
    println(options)
    println(remainingArgs)
