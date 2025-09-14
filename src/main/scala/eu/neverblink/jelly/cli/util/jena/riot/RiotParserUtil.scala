package eu.neverblink.jelly.cli.util.jena.riot

import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat
import org.apache.jena.riot.lang.LabelToNode
import org.apache.jena.riot.{RDFParser, RDFParserRegistry, RIOT}
import org.apache.jena.riot.system.StreamRDF

import java.io.InputStream

/** Utility for creating Jena RDF parsers in jelly-cli.
  */
object RiotParserUtil:
  def parse(
      resolveIris: Boolean,
      format: RdfFormat.Jena,
      source: InputStream,
      output: StreamRDF,
  ): Unit = {
    // Only really enable IRI resolution if the format supports it
    if resolveIris && format.supportsBaseIri then
      // Parser with full IRI resolution
      RDFParser.source(source)
        .lang(format.jenaLang)
        .labelToNode(LabelToNode.createUseLabelAsGiven())
        .checking(false)
        .strict(false)
        .parse(output)
    else
      // Fast parser with validation disabled
      RDFParserRegistry
        .getFactory(format.jenaLang)
        .create(format.jenaLang, FastParserProfile())
        .read(source, "", format.jenaLang.getContentType, output, RIOT.getContext)
  }
