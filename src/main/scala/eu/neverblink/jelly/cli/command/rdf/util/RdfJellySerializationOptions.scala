package eu.neverblink.jelly.cli.command.rdf.util

import caseapp.*
import eu.neverblink.jelly.cli.InvalidArgument
import eu.neverblink.jelly.core.proto.v1.{LogicalStreamType, PhysicalStreamType, RdfStreamOptions}
import eu.neverblink.jelly.core.utils.LogicalStreamTypeUtils
import eu.neverblink.jelly.core.JellyOptions

private val `default.opt.streamName`: String = ""
private val `default.opt.rdfStar`: Boolean = true
private val `default.opt.maxNameTableSize`: Int = JellyOptions.BIG_STRICT.getMaxNameTableSize
private val `default.opt.maxPrefixTableSize`: Int = JellyOptions.BIG_STRICT.getMaxPrefixTableSize
private val `default.opt.maxDatatypeTableSize`: Int =
  JellyOptions.BIG_STRICT.getMaxDatatypeTableSize

/** Options for serializing in Jelly-RDF */
case class RdfJellySerializationOptions(
    @HelpMessage("Name of the output stream (in metadata). Default: (empty)")
    `opt.streamName`: Option[String] = None,
    @HelpMessage(
      "Whether the stream may contain generalized triples, quads, or datasets. Default: (true for N-Triples/N-Quads and Jena binary formats, false otherwise)",
    )
    `opt.generalizedStatements`: Option[Boolean] = None,
    @HelpMessage(
      "Whether the stream may contain RDF-star statements. Default: " + `default.opt.rdfStar`,
    )
    `opt.rdfStar`: Option[Boolean] = None,
    @HelpMessage(
      "Maximum size of the name lookup table. Default: " + `default.opt.maxNameTableSize`,
    )
    `opt.maxNameTableSize`: Option[Int] = None,
    @HelpMessage(
      "Maximum size of the prefix lookup table. Default: " + `default.opt.maxPrefixTableSize`,
    )
    `opt.maxPrefixTableSize`: Option[Int] = None,
    @HelpMessage(
      "Maximum size of the datatype lookup table. Default: " + `default.opt.maxDatatypeTableSize`,
    )
    `opt.maxDatatypeTableSize`: Option[Int] = None,
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
  private object inferred:
    var options: Option[RdfStreamOptions] = None
    var generalized: Boolean = false

  def setOptions(rdfStreamOptions: RdfStreamOptions): Unit = inferred.options = Some(
    rdfStreamOptions,
  )

  def inferGeneralized(inputFormat: Option[String], filename: Option[String]): Unit =
    val explicitFormat = inputFormat.flatMap(RdfFormat.find)
    val implicitFormat = filename.flatMap(RdfFormat.inferFormat)
    inferred.generalized = (explicitFormat, implicitFormat) match {
      case (Some(f: RdfFormat.SupportsGeneralizedRdf), _) => true
      case (_, Some(f: RdfFormat.SupportsGeneralizedRdf)) => true
      case _ => false
    }

  private lazy val logicalType: Option[LogicalStreamType] =
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
    logicalType

  private lazy val physicalType: PhysicalStreamType =
    `opt.physicalType`.map(_.trim.toUpperCase) match
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

  private def makeStreamOptions(): RdfStreamOptions =
    RdfStreamOptions.newInstance()
      .setStreamName(`opt.streamName`.getOrElse(`default.opt.streamName`))
      .setGeneralizedStatements(`opt.generalizedStatements`.getOrElse(inferred.generalized))
      .setRdfStar(`opt.rdfStar`.getOrElse(`default.opt.rdfStar`))
      .setMaxNameTableSize(`opt.maxNameTableSize`.getOrElse(`default.opt.maxNameTableSize`))
      .setMaxPrefixTableSize(`opt.maxPrefixTableSize`.getOrElse(`default.opt.maxPrefixTableSize`))
      .setMaxDatatypeTableSize(
        `opt.maxDatatypeTableSize`.getOrElse(`default.opt.maxDatatypeTableSize`),
      )
      .setPhysicalType(physicalType)
      .setLogicalType(logicalType.getOrElse(LogicalStreamType.UNSPECIFIED))

  private lazy val optionsFromFileWithOverrides: Option[RdfStreamOptions] =
    inferred.options.map(x => {
      val cloned = x.clone()
      if `opt.generalizedStatements`.isDefined then
        cloned.setGeneralizedStatements(`opt.generalizedStatements`.get)
      if `opt.streamName`.isDefined then // comment to stop scalafmt from making this a mess
        cloned.setStreamName(`opt.streamName`.get)
      if `opt.rdfStar`.isDefined then // comment to stop scalafmt from making this a mess
        cloned.setRdfStar(`opt.rdfStar`.get)
      if `opt.maxNameTableSize`.isDefined then
        cloned.setMaxNameTableSize(`opt.maxNameTableSize`.get)
      if `opt.maxPrefixTableSize`.isDefined then
        cloned.setMaxPrefixTableSize(`opt.maxPrefixTableSize`.get)
      if `opt.maxDatatypeTableSize`.isDefined then
        cloned.setMaxDatatypeTableSize(`opt.maxDatatypeTableSize`.get)
      if `opt.physicalType`.isDefined then // comment to stop scalafmt from making this a mess
        cloned.setPhysicalType(physicalType)
      if `opt.logicalType`.isDefined then
        cloned.setLogicalType(logicalType.getOrElse(LogicalStreamType.UNSPECIFIED))
      cloned
    })

  lazy val asRdfStreamOptions: RdfStreamOptions =
    optionsFromFileWithOverrides.getOrElse(makeStreamOptions())
