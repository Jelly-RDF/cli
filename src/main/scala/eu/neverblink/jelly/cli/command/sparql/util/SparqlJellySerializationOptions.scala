package eu.neverblink.jelly.cli.command.sparql.util

import caseapp.*
import eu.neverblink.jelly.cli.InvalidArgument
import eu.neverblink.jelly.cli.util.args.StreamOptionArgs
import eu.neverblink.jelly.core.proto.v1.sparql.{SparqlResultsOptions, SparqlStreamType}
import eu.neverblink.jelly.core.sparql.JellySparqlOptions

private val defaultOptions: SparqlResultsOptions = JellySparqlOptions.BIG

/** Options for serializing in Jelly-SPARQL.
  *
  * This is the Jelly-SPARQL counterpart of
  * [[eu.neverblink.jelly.cli.command.rdf.util.RdfJellySerializationOptions]].
  */
case class SparqlJellySerializationOptions(
    @HelpMessage("Name of the output stream (in metadata). Default: (empty)")
    `opt.streamName`: Option[String] = None,
    @HelpMessage(
      "Type of the stream. One of: flat (one result set), punctuated (a sequence of result " +
        "sets). Default: flat, or punctuated if there is more than one input file",
    )
    `opt.streamType`: Option[String] = None,
    @HelpMessage(StreamOptionArgs.rdfVersionHelp + " Default: unspecified")
    `opt.rdfVersion`: Option[String] = None,
    @HelpMessage(
      "Maximum size of the name lookup table. Must be at least " +
        JellySparqlOptions.MIN_NAME_TABLE_SIZE + ". Default: " + defaultOptions.getMaxNameTableSize,
    )
    `opt.maxNameTableSize`: Option[Int] = None,
    @HelpMessage(
      "Maximum size of the prefix lookup table. Default: " + defaultOptions.getMaxPrefixTableSize,
    )
    `opt.maxPrefixTableSize`: Option[Int] = None,
    @HelpMessage(
      "Maximum size of the datatype lookup table. Default: " +
        defaultOptions.getMaxDatatypeTableSize,
    )
    `opt.maxDatatypeTableSize`: Option[Int] = None,
):

  /** The stream type set on the command line, if any. */
  def streamType: Option[SparqlStreamType] =
    `opt.streamType`.map(_.trim.toLowerCase).map {
      case "flat" => SparqlStreamType.FLAT
      case "punctuated" => SparqlStreamType.PUNCTUATED
      case _ =>
        throw InvalidArgument(
          "--opt.stream-type",
          `opt.streamType`.get,
          Some("Must be one of: flat, punctuated"),
        )
    }

  /** Builds the stream options, starting from `base` (or the default preset) and applying the
    * options set on the command line on top.
    *
    * @param base
    *   options to start from, e.g., copied from another Jelly-SPARQL file
    * @throws InvalidArgument
    *   if any of the options is invalid
    */
  def toSparqlResultsOptions(base: Option[SparqlResultsOptions] = None): SparqlResultsOptions =
    StreamOptionArgs.checkTableSize(
      "--opt.max-name-table-size",
      `opt.maxNameTableSize`,
      JellySparqlOptions.MIN_NAME_TABLE_SIZE,
    )
    StreamOptionArgs.checkTableSize("--opt.max-prefix-table-size", `opt.maxPrefixTableSize`, 0)
    StreamOptionArgs.checkTableSize(
      "--opt.max-datatype-table-size",
      `opt.maxDatatypeTableSize`,
      0,
    )
    val options = base.getOrElse(defaultOptions).clone()
    `opt.streamName`.foreach(options.setStreamName)
    streamType.foreach(options.setStreamType)
    `opt.rdfVersion`.map(StreamOptionArgs.rdfVersion).foreach(options.setRdfVersion)
    `opt.maxNameTableSize`.foreach(options.setMaxNameTableSize)
    `opt.maxPrefixTableSize`.foreach(options.setMaxPrefixTableSize)
    `opt.maxDatatypeTableSize`.foreach(options.setMaxDatatypeTableSize)
    options

  /** Whether any of the --opt.* options were set. */
  def isAnySet: Boolean =
    `opt.streamName`.isDefined || `opt.streamType`.isDefined || `opt.rdfVersion`.isDefined ||
      `opt.maxNameTableSize`.isDefined || `opt.maxPrefixTableSize`.isDefined ||
      `opt.maxDatatypeTableSize`.isDefined
