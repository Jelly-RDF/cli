package eu.neverblink.jelly.cli.command
import caseapp.*
import eu.neverblink.jelly.cli.JellyCommand

case class FromJellyRDFOptions()

object RDFFromJelly extends JellyCommand[FromJellyRDFOptions]:
  override def names: List[List[String]] = List(
    List("rdf-from-jelly"),
  )

  override def run(options: FromJellyRDFOptions, remainingArgs: RemainingArgs): Unit =
    println("rdf-from-jelly")
    println(options)
    println(remainingArgs)
    println("rdf-from-jelly")

  /*
   * This method will be used to validate the passing RDF stream
   */
  def validate(): Unit =
    println("rdf-from-jelly validate")
    println("rdf-from-jelly validate")
