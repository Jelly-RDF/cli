package eu.neverblink.jelly.cli.command.rdf.util

import caseapp.HelpMessage

/** Performance-related options for RDF processing.
  */
case class RdfPerformanceOptions(
    @HelpMessage(
      "Resolve IRIs with regard to the base specified in the input document. " +
        "Disabling this will result in faster parsing of Turtle, JSON-LD and RDF/XML, but will " +
        "also potentially result in relative IRIs in the output. " +
        "Default: true (ignored for formats that don't support base IRIs).",
    )
    resolveIris: Boolean = true,
    @HelpMessage(
      "Enable term validation (slower). Default: false for all commands except 'rdf validate'.",
    )
    validateTerms: Option[Boolean] = None,
)
