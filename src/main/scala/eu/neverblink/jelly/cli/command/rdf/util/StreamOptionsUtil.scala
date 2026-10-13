package eu.neverblink.jelly.cli.command.rdf.util

import eu.neverblink.jelly.cli.util.io.ProtoText
import eu.neverblink.jelly.core.JellyConstants
import eu.neverblink.jelly.core.proto.v1.{RdfStreamOptions, RdfVersion}

object StreamOptionsUtil:

  def prettyPrint(streamOptions: RdfStreamOptions): String =
    ProtoText.print(RdfStreamOptions.getDescriptor, streamOptions.toByteArray)

  /** Whether the options announce a Jelly-RDF 1.0 or 1.1 stream (row layout). Version 0 (not set)
    * means Jelly-RDF 1.2.
    */
  def isRowLayout(options: RdfStreamOptions): Boolean =
    JellyConstants.requestsRowLayout(options.getVersion)

  /** Whether a stream with these options may contain generalized statements. */
  def allowsGeneralized(options: RdfStreamOptions): Boolean =
    isRowLayout(options) && options.getGeneralizedStatements

  /** Whether a stream with these options may contain triple terms. */
  def allowsTripleTerms(options: RdfStreamOptions): Boolean =
    if isRowLayout(options) then options.getRdfStar
    else
      options.getRdfVersion == RdfVersion.RDF_VERSION_UNSPECIFIED ||
      options.getRdfVersion == RdfVersion.RDF_VERSION_1_2

  /** How to allow generalized statements in a stream with these options, for error messages. */
  def generalizedHint(options: RdfStreamOptions): String =
    if isRowLayout(options) then
      "Use --opt.generalized-statements=true to allow generalized statements."
    else
      "Jelly-RDF 1.2 cannot store generalized statements. Use --opt.jelly-version=1.1 " +
        "--opt.generalized-statements=true to write them in Jelly-RDF 1.1."

  /** How to allow triple terms in a stream with these options, for error messages. */
  def tripleTermsHint(options: RdfStreamOptions): String =
    if isRowLayout(options) then "Use --opt.triple-terms=true to allow triple terms."
    else "Use --opt.rdf-version=1.2 to allow triple terms."
