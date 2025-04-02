package eu.neverblink.jelly.cli.command.rdf

import caseapp.*
import eu.neverblink.jelly.cli.InvalidArgument
import eu.ostrzyciel.jelly.core.{JellyOptions, LogicalStreamTypeFactory}
import eu.ostrzyciel.jelly.core.proto.v1.{LogicalStreamType, RdfStreamOptions}

/** Options for serializing in Jelly-RDF */
case class RdfJellySerializationOptions(
    @HelpMessage("Name of the output stream (in metadata). Default: (empty)")
    `opt.streamName`: String = "",
    @HelpMessage(
      "Whether the stream may contain generalized triples, quads, or datasets. Default: true",
    )
    `opt.generalizedStatements`: Boolean = true,
    @HelpMessage("Whether the stream may contain RDF-star statements. Default: true")
    `opt.rdfStar`: Boolean = true,
    @HelpMessage(
      "Maximum size of the name lookup table. Default: " + JellyOptions.bigStrict.maxNameTableSize,
    )
    `opt.maxNameTableSize`: Int = JellyOptions.bigStrict.maxNameTableSize,
    @HelpMessage(
      "Maximum size of the prefix lookup table. Default: " + JellyOptions.bigStrict.maxPrefixTableSize,
    )
    `opt.maxPrefixTableSize`: Int = JellyOptions.bigStrict.maxPrefixTableSize,
    @HelpMessage(
      "Maximum size of the datatype lookup table. Default: " + JellyOptions.bigStrict.maxDatatypeTableSize,
    )
    `opt.maxDatatypeTableSize`: Int = JellyOptions.bigStrict.maxDatatypeTableSize,
    @HelpMessage(
      "Logical (RDF-STaX-based) stream type. This can be either a name like " +
        "`FLAT_QUADS` or a full IRI like `https://w3id.org/stax/ontology#flatQuadStream`. " +
        "Default: (unspecified)",
    )
    `opt.logicalType`: Option[String] = None,
):
  lazy val asRdfStreamOptions: RdfStreamOptions =
    val logicalIri = `opt.logicalType`
      .map(_.trim).filter(_.nonEmpty)
      .map {
        case x if x.startsWith("http") => x
        case x if x.toUpperCase.endsWith("S") =>
          val words = x.substring(0, x.length - 1).split("_").map(_.toLowerCase)
          val wordSeq = words.head +: words.tail.map(_.capitalize)
          "https://w3id.org/stax/ontology#" + wordSeq.mkString + "Stream"
        case _ => "" // invalid IRI, we'll catch it in the next step
      }
    val logicalType = logicalIri.flatMap(LogicalStreamTypeFactory.fromOntologyIri)
    if logicalIri.isDefined && logicalType.isEmpty then
      throw InvalidArgument(
        "--opt.logical-type",
        `opt.logicalType`.get,
        Some("Logical type must be either a full RDF-STaX IRI or a name like `FLAT_QUADS`"),
      )
    RdfStreamOptions(
      streamName = `opt.streamName`,
      generalizedStatements = `opt.generalizedStatements`,
      rdfStar = `opt.rdfStar`,
      maxNameTableSize = `opt.maxNameTableSize`,
      maxPrefixTableSize = `opt.maxPrefixTableSize`,
      maxDatatypeTableSize = `opt.maxDatatypeTableSize`,
      logicalType = logicalType.getOrElse(LogicalStreamType.UNSPECIFIED),
    )
