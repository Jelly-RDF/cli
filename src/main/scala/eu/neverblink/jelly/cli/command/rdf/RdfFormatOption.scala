package eu.neverblink.jelly.cli.command.rdf

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.riot.RDFLanguages

enum RdfFormatOption(
    val cliOptions: List[String],
    val fullName: String,
):
  case NQuads
      extends RdfFormatOption(
        List("nq", "nt", "nquads", "ntriples"),
        "N-Quads",
      )
  case JellyBinary
      extends RdfFormatOption(
        List("jelly"),
        "Jelly binary format",
      )
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

  /** Infers the format based on the file name.
    */
  def inferFormat(fileName: String): Option[RdfFormatOption] = {
    RDFLanguages.guessContentType(fileName) match {
      case contentType if contentType == RDFLanguages.NQUADS.getContentType =>
        Some(RdfFormatOption.NQuads)
      case contentType if contentType == JellyLanguage.JELLY.getContentType =>
        Some(RdfFormatOption.JellyBinary)
      case _ if fileName.endsWith(".jelly.txt") => Some(RdfFormatOption.JellyText)
      case _ => None
    }
  }
