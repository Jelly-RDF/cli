package eu.neverblink.jelly.cli.command.sparql.util

import eu.neverblink.jelly.convert.jena.sparql.JellySparqlLanguage
import org.apache.jena.riot.{Lang, RDFLanguages}
import org.apache.jena.riot.resultset.ResultSetLang

/** A SPARQL result set format that the sparql commands can convert to or from.
  *
  * This is the SPARQL results counterpart of
  * [[eu.neverblink.jelly.cli.command.rdf.util.RdfFormat]].
  */
sealed trait SparqlFormat:
  val fullName: String
  val cliOptions: List[String]
  override final def toString: String = fullName

object SparqlFormat:

  /** Formats we can read a result set from. */
  sealed trait Readable extends SparqlFormat

  /** Formats we can write a result set to. */
  sealed trait Writeable extends SparqlFormat

  /** Formats handled by Jena's result set readers and writers. */
  sealed trait Jena extends SparqlFormat:
    val jenaLang: Lang

  case object Json extends SparqlFormat.Jena, SparqlFormat.Readable, SparqlFormat.Writeable:
    override val fullName: String = "SPARQL results JSON"
    override val cliOptions: List[String] = List("json", "srj")
    override val jenaLang: Lang = ResultSetLang.RS_JSON

  case object Xml extends SparqlFormat.Jena, SparqlFormat.Readable, SparqlFormat.Writeable:
    override val fullName: String = "SPARQL results XML"
    override val cliOptions: List[String] = List("xml", "srx")
    override val jenaLang: Lang = ResultSetLang.RS_XML

  case object Csv extends SparqlFormat.Jena, SparqlFormat.Readable, SparqlFormat.Writeable:
    override val fullName: String = "CSV"
    override val cliOptions: List[String] = List("csv")
    override val jenaLang: Lang = ResultSetLang.RS_CSV

  case object Tsv extends SparqlFormat.Jena, SparqlFormat.Readable, SparqlFormat.Writeable:
    override val fullName: String = "TSV"
    override val cliOptions: List[String] = List("tsv")
    override val jenaLang: Lang = ResultSetLang.RS_TSV

  /** Jena's pretty-printed table. Jena registers no reader for it, so it's output-only. */
  case object Text extends SparqlFormat.Jena, SparqlFormat.Writeable:
    override val fullName: String = "Text table"
    override val cliOptions: List[String] = List("text")
    override val jenaLang: Lang = ResultSetLang.RS_Text

  /** We never convert Jelly to Jelly, so this is neither Readable nor Writeable – it is only here
    * so that the other side of the conversion has a name.
    */
  case object JellySparql extends SparqlFormat.Jena:
    override val fullName: String = "Jelly-SPARQL"
    override val cliOptions: List[String] = List("jelly-sparql")
    override val jenaLang: Lang = JellySparqlLanguage.JELLY_SPARQL

  case object JellySparqlText extends SparqlFormat.Readable, SparqlFormat.Writeable:
    override val fullName: String = "Jelly-SPARQL text"
    override val cliOptions: List[String] = List("jelly-sparql-text")
    val extension = ".jellys.txt"

  private val sparqlFormats: List[SparqlFormat] =
    List(Json, Xml, Csv, Tsv, Text, JellySparql, JellySparqlText)

  def all: List[SparqlFormat] = sparqlFormats

  lazy val readable: List[SparqlFormat.Readable] =
    sparqlFormats.collect { case f: SparqlFormat.Readable => f }

  lazy val writeable: List[SparqlFormat.Writeable] =
    sparqlFormats.collect { case f: SparqlFormat.Writeable => f }

  /** Returns a string representation of the option for the user.
    */
  def optionString(option: SparqlFormat): String =
    f"${option.fullName}: ${option.cliOptions.mkString(", ")}"

  def validFormatsString(formats: List[SparqlFormat]): String =
    formats.map(optionString).mkString("; ")

  def helpMsg(formats: List[SparqlFormat], default: SparqlFormat): String =
    f"Possible values: ${validFormatsString(formats)}. Default: ${default.fullName}"

  /** Finds the appropriate SparqlFormat based on supplied option string.
    */
  def find(cliOption: String): Option[SparqlFormat] =
    sparqlFormats.find(_.cliOptions.contains(cliOption))

  /** Infers the format based on the file name.
    */
  def inferFormat(fileName: String): Option[SparqlFormat] =
    if fileName.endsWith(JellySparqlText.extension) then Some(JellySparqlText)
    else
      val guessType = RDFLanguages.guessContentType(fileName)
      sparqlFormats.collectFirst {
        case f: SparqlFormat.Jena if f.jenaLang.getContentType == guessType => f
      }
