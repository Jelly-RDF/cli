package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.command.rdf.RdfFormatOption.JellyBinary

trait RdfCommandPrintUtil:
  val validFormats: List[RdfFormatOption] =
    RdfFormatOption.values.filterNot(_ == JellyBinary).toList
  val defaultFormat: RdfFormatOption

  /** Prints the available RDF formats to the user.
    */
  lazy val validFormatsString: String =
    validFormats.map(RdfFormatOption.optionString).mkString(", ")

  lazy val helpMsg: String =
    f"Possible values: ${validFormatsString}. Default format: ${defaultFormat.fullName}"
