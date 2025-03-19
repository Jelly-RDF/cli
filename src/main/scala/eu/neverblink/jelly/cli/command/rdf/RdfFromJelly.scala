package eu.neverblink.jelly.cli.command.rdf
import caseapp.*
import eu.neverblink.jelly.cli.JellyCommand

case class FromJellyRdfOptions()

object RdfFromJelly extends JellyCommand[FromJellyRdfOptions]:
  override def names: List[List[String]] = List(
    List("rdf", "from-jelly"),
  )

  override def run(options: FromJellyRdfOptions, remainingArgs: RemainingArgs): Unit =
    // This is a placeholder for the actual implementation
    println("rdf from-jelly")
    println(options)
    println(remainingArgs)
