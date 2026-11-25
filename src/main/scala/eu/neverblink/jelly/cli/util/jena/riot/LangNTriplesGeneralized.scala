package eu.neverblink.jelly.cli.util.jena.riot

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.system.{ParserProfile, StreamRDF}
import org.apache.jena.riot.tokens.{Token, TokenType, Tokenizer}
import org.apache.jena.riot.{Lang, RDFLanguages}
import org.apache.jena.sparql.util.Context

/** Parser for generalized N-Triples. Heavily inspired by the Jena Riot code:
  * https://github.com/apache/jena/blob/bd97ad4cf731ade857926787dd2df735644a354b/jena-arq/src/main/java/org/apache/jena/riot/lang/LangNTriples.java
  */
final class LangNTriplesGeneralized(
    tokens: Tokenizer,
    profile: ParserProfile,
    dest: StreamRDF,
    context: Context,
) extends LangNTupleGeneralized[Triple](tokens, profile, dest, context):

  override def getLang: Lang = RDFLanguages.NTRIPLES

  /** Method to parse the whole stream of triples, sending each to the sink */
  override protected def runParser(): Unit =
    while (hasNext) {
      val x = parseOneStatement
      if (x != null) dest.triple(x)
    }

  override protected def parseOneStatement: Triple =
    if (parsePossibleVersion) return null
    val triple = parseTripleGeneralized
    val x = nextToken
    if (x.getType ne TokenType.DOT) exception(x, "Triple not terminated by DOT: %s", x)
    triple

  override protected def tokenAsNode(token: Token): Node =
    profile.create(null, token)
