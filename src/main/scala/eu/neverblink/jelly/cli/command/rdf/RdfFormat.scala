package eu.neverblink.jelly.cli.command.rdf

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.riot.{Lang, RDFLanguages}

sealed trait RdfFormat:
  val fullName: String
  val cliOptions: List[String]

object RdfFormat:

  sealed trait Writeable extends RdfFormat

  sealed trait Jena extends RdfFormat:
    val jenaLang: Lang

  object Jena:
    sealed trait Writeable extends Jena, RdfFormat.Writeable
    sealed trait Readable extends Jena

  case object NQuads extends RdfFormat.Jena.Writeable, RdfFormat.Jena.Readable:
    override val fullName: String = "N-Quads"
    override val cliOptions: List[String] = List("nq", "nquads")
    override val jenaLang: Lang = RDFLanguages.NQUADS

  case object NTriples extends RdfFormat.Jena.Writeable, RdfFormat.Jena.Readable:
    override val fullName: String = "N-Triples"
    override val cliOptions: List[String] = List("nt", "ntriples")
    override val jenaLang: Lang = RDFLanguages.NTRIPLES

  case object Turtle extends RdfFormat.Jena.Writeable, RdfFormat.Jena.Readable:
    override val fullName: String = "Turtle"
    override val cliOptions: List[String] = List("ttl", "turtle")
    override val jenaLang: Lang = RDFLanguages.TURTLE

  case object TriG extends RdfFormat.Jena.Writeable, RdfFormat.Jena.Readable:
    override val fullName: String = "TriG"
    override val cliOptions: List[String] = List("trig")
    override val jenaLang: Lang = RDFLanguages.TRIG

  case object RdfProto extends RdfFormat.Jena.Writeable, RdfFormat.Jena.Readable:
    override val fullName: String = "RDF Protobuf"
    override val cliOptions: List[String] = List("rdfp", "rdfproto", "rdf-proto")
    override val jenaLang: Lang = RDFLanguages.RDFPROTO

  case object Thrift extends RdfFormat.Jena.Writeable, RdfFormat.Jena.Readable:
    override val fullName: String = "RDF Thrift"
    override val cliOptions: List[String] = List("rdfthrift", "thrift")
    override val jenaLang: Lang = RDFLanguages.RDFTHRIFT

  case object RdfXML extends RdfFormat.Jena.Readable:
    override val fullName: String = "RDF/XML"
    override val cliOptions: List[String] = List("rdfxml", "rdf-xml")
    override val jenaLang: Lang = RDFLanguages.RDFXML

  case object JsonLD extends RdfFormat.Jena.Readable:
    override val fullName: String = "JSON-LD"
    override val cliOptions: List[String] = List("jsonld", "json-ld")
    override val jenaLang: Lang = RDFLanguages.JSONLD

  // We do not ever want to write or read from Jelly to Jelly
  // So better not have it as Writeable or Readable, just mark that it's integrated into Jena
  case object JellyBinary extends RdfFormat.Jena:
    override val fullName: String = "Jelly binary format"
    override val cliOptions: List[String] = List("jelly")
    override val jenaLang: Lang = JellyLanguage.JELLY

  case object JellyText extends RdfFormat, RdfFormat.Writeable:
    override val fullName: String = "Jelly text format"
    override val cliOptions: List[String] = List("jelly-text")
    val extension = ".jelly.txt"

  private val rdfFormats: List[RdfFormat] =
    List(NQuads, NTriples, JellyBinary, JellyText, Turtle, TriG, RdfProto, Thrift, RdfXML, JsonLD)

  def all: List[RdfFormat] = rdfFormats

  /** Returns a string representation of the option for the user.
    */
  def optionString(option: RdfFormat): String =
    f"${option.cliOptions.map(s => f"\"${s}\"").mkString(", ")} for ${option.fullName}"

  /** Finds the appropriate RdfFormat based on supplied option string.
    */
  def find(cliOption: String): Option[RdfFormat] =
    rdfFormats.find(_.cliOptions.contains(cliOption))

  /** Infers the format based on the file name.
    */
  def inferFormat(fileName: String): Option[RdfFormat] =
    val jenaImpl = RdfFormat.all.collect({ case x: RdfFormat.Jena => x })
    val guessType = RDFLanguages.guessContentType(fileName)
    val formatGuessed = jenaImpl.collectFirst({
      case x if x.jenaLang.getContentType == guessType => x
    })
    formatGuessed match {
      case Some(f: RdfFormat.Jena) => formatGuessed
      case _ if fileName.endsWith(JellyText.extension) => Some(RdfFormat.JellyText)
      case _ => None
    }
