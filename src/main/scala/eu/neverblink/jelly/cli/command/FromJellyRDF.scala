package eu.neverblink.jelly.cli.command
import caseapp.*
import eu.neverblink.jelly.cli.JellyCommand

case class FromJellyRDFOptions()

object FromJellyRDF extends JellyCommand[FromJellyRDFOptions]:
  override def names: List[List[String]] = List(
    List("from-jelly-rdf"),
  )

  override def run(options: FromJellyRDFOptions, remainingArgs: RemainingArgs): Unit =
    println("from-jelly-rdf")
    println(options)
    println(remainingArgs)
    println("from-jelly-rdf")

  /*
   * This method will be used to validate the passing RDF stream
   */
  def validate(): Unit =
    println("from-jelly-rdf validate")
    println("from-jelly-rdf validate")
