package eu.neverblink.jelly.cli.util.jena.riot

import org.apache.jena.riot.{Lang, RDFParser, RDFParserRegistry, RIOT}
import org.apache.jena.riot.system.StreamRDF

import java.io.InputStream

/** Utility for creating Jena RDF parsers in jelly-cli.
  */
object RiotParserUtil:
  def parse(
      enableTermValidation: Boolean,
      lang: Lang,
      source: InputStream,
      output: StreamRDF,
  ): Unit =
    if enableTermValidation then
      // Standard parser with validation enabled
      RDFParser.source(source)
        .lang(lang)
        .parse(output)
    else
      // Fast parser with validation disabled
      RDFParserRegistry
        .getFactory(lang)
        .create(lang, FastParserProfile())
        .read(source, "", lang.getContentType, output, RIOT.getContext)
