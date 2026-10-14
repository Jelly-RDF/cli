package eu.neverblink.jelly.cli.command.rdf.util

import caseapp.*
import eu.neverblink.jelly.cli.InvalidArgument
import eu.neverblink.jelly.cli.util.args.StreamOptionArgs
import eu.neverblink.jelly.convert.jena.riot.JellyFormatVariant
import eu.neverblink.jelly.core.proto.v1.*
import eu.neverblink.jelly.core.utils.LogicalStreamTypeUtils
import eu.neverblink.jelly.core.{JellyConstants, JellyOptions}
import eu.neverblink.jelly.core.internal.BaseJellyOptions

private val `default.opt.streamName`: String = ""
private val `default.opt.tripleTerms`: Boolean = true
private val `default.opt.maxNameTableSize`: Int = JellyOptions.BIG_STRICT.getMaxNameTableSize
private val `default.opt.maxPrefixTableSize`: Int = JellyOptions.BIG_STRICT.getMaxPrefixTableSize
private val `default.opt.maxDatatypeTableSize`: Int =
  JellyOptions.BIG_STRICT.getMaxDatatypeTableSize

object RdfJellySerializationOptions:
  def defaultRowsPerFrame(rowLayout: Boolean): Int =
    if rowLayout then 256 else JellyFormatVariant.DEFAULT_FRAME_SIZE

  def defaultOptions: RdfStreamOptions = RdfJellySerializationOptions().asRdfStreamOptions

/** Options for serializing in Jelly-RDF */
case class RdfJellySerializationOptions(
    @HelpMessage(
      "Version of Jelly-RDF to write. One of: 1.2, 1.1. Jelly-RDF 1.1 is deprecated, use it " +
        "only for readers that do not support 1.2. Default: 1.2",
    )
    `opt.jellyVersion`: Option[String] = None,
    @HelpMessage("Name of the output stream (in metadata). Default: (empty)")
    `opt.streamName`: Option[String] = None,
    @HelpMessage(
      "Physical stream type. One of: TRIPLES, QUADS, GRAPHS (only Jelly-RDF 1.1). " +
        "Default: either TRIPLES or QUADS, depending on the input format.",
    )
    `opt.physicalType`: Option[String] = None,
    @HelpMessage(
      "Only Jelly-RDF 1.2. Type of the stream. One of: flat (one RDF graph or dataset), " +
        "messages (a stream of RDF Messages). Default: flat",
    )
    `opt.streamType`: Option[String] = None,
    @HelpMessage(
      "Only Jelly-RDF 1.2. " + StreamOptionArgs.rdfVersionHelp + " Default: unspecified",
    )
    `opt.rdfVersion`: Option[String] = None,
    @HelpMessage(
      "Maximum size of the name lookup table. Must be at least " +
        BaseJellyOptions.MIN_COLUMN_NAME_TABLE_SIZE + " in Jelly-RDF 1.2. Default: " +
        `default.opt.maxNameTableSize`,
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
      "Only Jelly-RDF 1.1. Whether the stream may contain generalized triples, quads, or " +
        "datasets. Default: (true for N-Triples/N-Quads and Jena binary formats, false otherwise)",
    )
    `opt.generalizedStatements`: Option[Boolean] = None,
    @HelpMessage(
      "Only Jelly-RDF 1.1. Whether the stream may contain RDF 1.2 triple terms. Default: " +
        `default.opt.tripleTerms`,
    )
    `opt.tripleTerms`: Option[Boolean] = None,
    @HelpMessage(
      "Only Jelly-RDF 1.1. Logical (RDF-STaX-based) stream type. This can be either a name " +
        "like `FLAT_QUADS` or a full IRI like `https://w3id.org/stax/ontology#flatQuadStream`. " +
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

  /** Whether the output is Jelly-RDF 1.0 or 1.1 (row layout), rather than Jelly-RDF 1.2. */
  lazy val isRowLayout: Boolean =
    `opt.jellyVersion`.map(_.trim) match
      case Some("1.1") => true
      case Some("1.2") => false
      case Some(x) =>
        throw InvalidArgument("--opt.jelly-version", x, Some("Must be one of: 1.2, 1.1"))
      case None => inferred.options.exists(StreamOptionsUtil.isRowLayout)

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

  private lazy val streamType: Option[RdfStreamType] =
    `opt.streamType`.map(_.trim.toLowerCase).map {
      case "flat" => RdfStreamType.FLAT
      case "messages" => RdfStreamType.MESSAGES
      case _ =>
        throw InvalidArgument(
          "--opt.stream-type",
          `opt.streamType`.get,
          Some("Must be one of: flat, messages"),
        )
    }

  /** Throws if an option of one version of Jelly-RDF is set while writing the other one. */
  private def checkVersionSpecificOptions(): Unit =
    def onlyIn(version: String, argument: String, value: Option[Any], hint: String): Unit =
      value.foreach { v =>
        throw InvalidArgument(
          argument,
          v.toString,
          Some(s"This option is only valid in Jelly-RDF $version output. $hint"),
        )
      }
    if isRowLayout then
      val hint = "Use --opt.jelly-version=1.2 to write Jelly-RDF 1.2."
      onlyIn("1.2", "--opt.stream-type", `opt.streamType`, hint)
      onlyIn("1.2", "--opt.rdf-version", `opt.rdfVersion`, hint)
    else
      val hint = "Use --opt.jelly-version=1.1 to write Jelly-RDF 1.1."
      onlyIn(
        "1.1",
        "--opt.generalized-statements",
        `opt.generalizedStatements`,
        "Jelly-RDF 1.2 cannot store generalized statements. " + hint,
      )
      onlyIn(
        "1.1",
        "--opt.triple-terms",
        `opt.tripleTerms`,
        "In Jelly-RDF 1.2, use --opt.rdf-version instead. " + hint,
      )
      onlyIn(
        "1.1",
        "--opt.logical-type",
        `opt.logicalType`,
        "In Jelly-RDF 1.2, use --opt.stream-type instead. " + hint,
      )
      onlyIn(
        "1.1",
        "--opt.physical-type",
        `opt.physicalType`.filter(_ => physicalType == PhysicalStreamType.GRAPHS),
        "In Jelly-RDF 1.2, use QUADS instead. " + hint,
      )
      StreamOptionArgs.checkTableSize(
        "--opt.max-name-table-size",
        `opt.maxNameTableSize`,
        BaseJellyOptions.MIN_COLUMN_NAME_TABLE_SIZE,
      )

  /** The options to start from, before the ones set on the command line: those of the
    * --options-from file, made fit for the output version, or the defaults.
    */
  private def baseOptions: RdfStreamOptions.Mutable =
    val defaults = RdfStreamOptions.newInstance()
      .setStreamName(`default.opt.streamName`)
      .setMaxNameTableSize(`default.opt.maxNameTableSize`)
      .setMaxPrefixTableSize(`default.opt.maxPrefixTableSize`)
      .setMaxDatatypeTableSize(`default.opt.maxDatatypeTableSize`)
    val rowLayoutDefaults = (o: RdfStreamOptions.Mutable) =>
      o.setGeneralizedStatements(inferred.generalized).setRdfStar(`default.opt.tripleTerms`)
    inferred.options match
      case None => if isRowLayout then rowLayoutDefaults(defaults) else defaults
      case Some(fromFile) if StreamOptionsUtil.isRowLayout(fromFile) == isRowLayout =>
        fromFile.clone()
      case Some(fromFile) =>
        // The file is of the other version of Jelly-RDF, so only take the options both have
        defaults
          .setStreamName(fromFile.getStreamName)
          .setPhysicalType(fromFile.getPhysicalType match
            case PhysicalStreamType.GRAPHS => PhysicalStreamType.QUADS
            case t => t)
          .setMaxNameTableSize(fromFile.getMaxNameTableSize)
          .setMaxPrefixTableSize(fromFile.getMaxPrefixTableSize)
          .setMaxDatatypeTableSize(fromFile.getMaxDatatypeTableSize)
        if isRowLayout then rowLayoutDefaults(defaults) else defaults

  lazy val asRdfStreamOptions: RdfStreamOptions =
    checkVersionSpecificOptions()
    val options = baseOptions
    `opt.streamName`.foreach(options.setStreamName)
    if `opt.physicalType`.isDefined then options.setPhysicalType(physicalType)
    `opt.maxNameTableSize`.foreach(options.setMaxNameTableSize)
    `opt.maxPrefixTableSize`.foreach(options.setMaxPrefixTableSize)
    `opt.maxDatatypeTableSize`.foreach(options.setMaxDatatypeTableSize)
    if isRowLayout then
      `opt.generalizedStatements`.foreach(options.setGeneralizedStatements)
      `opt.tripleTerms`.foreach(options.setRdfStar)
      if `opt.logicalType`.isDefined then
        options.setLogicalType(logicalType.getOrElse(LogicalStreamType.UNSPECIFIED))
      // The writer sets the exact version (1.0 or 1.1), depending on the namespace declarations
      options.setVersion(JellyConstants.PROTO_VERSION_1_1_X)
    else
      streamType.foreach(options.setStreamType)
      `opt.rdfVersion`.map(StreamOptionArgs.rdfVersion).foreach(options.setRdfVersion)
      options.setVersion(JellyConstants.PROTO_VERSION_1_2_X)
