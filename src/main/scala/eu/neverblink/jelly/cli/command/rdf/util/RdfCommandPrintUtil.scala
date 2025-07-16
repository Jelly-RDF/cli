package eu.neverblink.jelly.cli.command.rdf.util

import scala.reflect.Typeable

trait RdfCommandPrintUtil[F <: RdfFormat: Typeable]:
  val defaultFormat: RdfFormat

  lazy val validFormats: List[RdfFormat] = RdfFormat.all.collect { case x: F => x }

  /** Prints the available RDF formats to the user.
    */
  lazy val validFormatsString: String =
    validFormats.map(RdfFormat.optionString).mkString("; ")

  lazy val helpMsg: String =
    f"Possible values: ${validFormatsString}. Default: ${defaultFormat.fullName}"
