package eu.neverblink.jelly.cli.command.rdf

trait RdfCommandPrintUtil:
  val validFormats: List[RdfFormat]
  val defaultFormat: RdfFormat

  /** Prints the available RDF formats to the user.
    */
  lazy val validFormatsString: String =
    validFormats.map(RdfFormat.optionString).mkString(", ")

  lazy val helpMsg: String =
    f"Possible values: ${validFormatsString}. Default format: ${defaultFormat.fullName}"
