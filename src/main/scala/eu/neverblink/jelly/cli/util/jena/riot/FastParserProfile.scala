package eu.neverblink.jelly.cli.util.jena.riot

import org.apache.jena.irix.IRIxResolver
import org.apache.jena.riot.RIOT
import org.apache.jena.riot.lang.LabelToNode
import org.apache.jena.riot.system.*

/** Jena RIOT parser profile with optimizations for speed:
  *   - No IRI resolution
  *   - No error logging
  *   - Passing blank node labels as-is
  *   - No extra checks
  */
final class FastParserProfile
    extends ParserProfileStd(
      FactoryRDFCaching(FactoryRDFCaching.DftNodeCacheSize, LabelToNode.createUseLabelAsGiven()),
      ErrorHandlerFactory.errorHandlerNoLogging,
      IRIxResolver.create().noBase().resolve(false).allowRelative(true).build(),
      PrefixMapStd(),
      RIOT.getContext,
      false,
      false,
    ):

  /** Skip IRI resolution for speed.
    */
  override def resolveIRI(uriStr: String, line: Long, col: Long): String = uriStr
