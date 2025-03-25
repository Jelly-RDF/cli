package eu.neverblink.jelly.cli.command.rdf

enum RdfFormatOption(val cliOptions: List[String], val fullName: String):
  case NQuads extends RdfFormatOption(List("nq", "nt", "nquads", "ntriples"), "N-Quads")
  case JellyBinary extends RdfFormatOption(List("jelly"), "Jelly binary format")
  case JellyText extends RdfFormatOption(List("jelly-text"), "Jelly text format")

object RdfFormatOption:
  /** Returns a string representation of the option for the user.
    */
  def optionString(option: RdfFormatOption): String =
    f"${option.cliOptions.map(s => f"\"${s}\"").mkString(", ")} for ${option.fullName}"

  /** Finds the appropriate RdfFormatOption based on supplied option string.
    */
  def find(cliOption: String): Option[RdfFormatOption] =
    RdfFormatOption.values.find(_.cliOptions.contains(cliOption))
