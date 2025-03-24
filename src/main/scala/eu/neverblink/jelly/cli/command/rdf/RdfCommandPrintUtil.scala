package eu.neverblink.jelly.cli.command.rdf

trait RdfCommandPrintUtil:
  val validFormats: List[RdfFormatOption]
  val defaultFormat: RdfFormatOption

  /** Formats the available RDF formats to the user.
    */
  lazy val validFormatsString: String =
    validFormats.map(RdfFormatOption.optionString).mkString(", ")

  lazy val helpMsg: String =
    f"Possible values: ${validFormatsString}. Default format: ${defaultFormat.fullName}"
