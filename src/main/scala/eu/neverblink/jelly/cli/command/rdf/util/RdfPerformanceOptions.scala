package eu.neverblink.jelly.cli.command.rdf.util

import caseapp.HelpMessage

/** Performance-related options for RDF processing.
  */
case class RdfPerformanceOptions(
    @HelpMessage(
      "Enable term validation and IRI resolution (slower). Default: false for all commands except 'rdf validate'.",
    )
    validateTerms: Option[Boolean] = None,
)
