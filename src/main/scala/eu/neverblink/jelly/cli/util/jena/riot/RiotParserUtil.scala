package eu.neverblink.jelly.cli.util.jena.riot

import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat
import org.apache.jena.irix.IRIs
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
      // Parser with full IRI resolution.
      // We set the base explicitly to the system base (the current working directory), which is
      // what Jena uses by default for stream-based Turtle/TriG parsing. Without this, readers that
      // do their own base handling (notably RDF/XML via ARP) receive a null base and fail to
      // resolve relative IRIs (e.g. "#foo"), throwing "Relative URI encountered".
      RDFParser.source(source)
        .lang(format.jenaLang)
        .base(IRIs.getBaseStr)
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
