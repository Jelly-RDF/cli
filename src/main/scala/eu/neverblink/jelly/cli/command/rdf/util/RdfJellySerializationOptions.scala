package eu.neverblink.jelly.cli.command.rdf.util

import caseapp.*
import eu.neverblink.jelly.cli.InvalidArgument
import eu.neverblink.jelly.core.proto.v1.{LogicalStreamType, PhysicalStreamType, RdfStreamOptions}
import eu.neverblink.jelly.core.utils.LogicalStreamTypeUtils
import eu.neverblink.jelly.core.JellyOptions

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
      "Maximum size of the name lookup table. Default: " + JellyOptions.BIG_STRICT.getMaxNameTableSize,
    )
    `opt.maxNameTableSize`: Int = JellyOptions.BIG_STRICT.getMaxNameTableSize,
    @HelpMessage(
      "Maximum size of the prefix lookup table. Default: " + JellyOptions.BIG_STRICT.getMaxPrefixTableSize,
    )
    `opt.maxPrefixTableSize`: Int = JellyOptions.BIG_STRICT.getMaxPrefixTableSize,
    @HelpMessage(
      "Maximum size of the datatype lookup table. Default: " + JellyOptions.BIG_STRICT.getMaxDatatypeTableSize,
    )
    `opt.maxDatatypeTableSize`: Int = JellyOptions.BIG_STRICT.getMaxDatatypeTableSize,
    @HelpMessage(
      "Physical stream type. One of: TRIPLES, QUADS, GRAPHS. " +
        "Default: either TRIPLES or QUADS, depending on the input format.",
    )
    `opt.physicalType`: Option[String] = None,
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
    val logicalType = logicalIri.flatMap({ iri =>
      Option(LogicalStreamTypeUtils.fromOntologyIri(iri))
    })
    if logicalIri.isDefined && logicalType.isEmpty then
      throw InvalidArgument(
        "--opt.logical-type",
        `opt.logicalType`.get,
        Some("Logical type must be either a full RDF-STaX IRI or a name like `FLAT_QUADS`"),
      )
    val physicalType = `opt.physicalType`.map(_.trim.toUpperCase) match
      case Some("TRIPLES") => PhysicalStreamType.TRIPLES
      case Some("QUADS") => PhysicalStreamType.QUADS
      case Some("GRAPHS") => PhysicalStreamType.GRAPHS
      case Some(x) =>
        throw InvalidArgument(
          "--opt.physical-type",
          x,
          Some("Physical type must be one of: TRIPLES, QUADS, GRAPHS"),
        )
      case None => PhysicalStreamType.UNSPECIFIED
    RdfStreamOptions.newInstance()
      .setStreamName(`opt.streamName`)
      .setGeneralizedStatements(`opt.generalizedStatements`)
      .setRdfStar(`opt.rdfStar`)
      .setMaxNameTableSize(`opt.maxNameTableSize`)
      .setMaxPrefixTableSize(`opt.maxPrefixTableSize`)
      .setMaxDatatypeTableSize(`opt.maxDatatypeTableSize`)
      .setPhysicalType(physicalType)
      .setLogicalType(logicalType.getOrElse(LogicalStreamType.UNSPECIFIED))
