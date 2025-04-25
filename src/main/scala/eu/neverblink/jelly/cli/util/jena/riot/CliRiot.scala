package eu.neverblink.jelly.cli.util.jena.riot

import org.apache.jena.atlas.web.ContentType
import org.apache.jena.riot.{RDFLanguages, RDFParserRegistry, ReaderRIOT}
import org.apache.jena.riot.lang.{LangRIOT, RiotParsers}
import org.apache.jena.riot.system.{ParserProfile, StreamRDF}
import org.apache.jena.riot.tokens.{Tokenizer, TokenizerText}
import org.apache.jena.sparql.util.Context

import java.io.{InputStream, Reader}

/** Registration utilities for jelly-cli's overrides of Apache Jena's Riot components (e.g.,
  * parsers).
  *
  * The initialize() method must be called before using any of the parsers, right after
  * JenaSystem.init().
  */
object CliRiot:
  private var initialized = false

  def initialize(): Unit = CliRiot.synchronized {
    if initialized then return
    RiotParsers.factoryNT = (_, parserProfile) => NTriplesReader(parserProfile)
    RiotParsers.factoryNQ = (_, parserProfile) => NQuadsReader(parserProfile)
    RDFParserRegistry.registerLangTriples(RDFLanguages.NTRIPLES, RiotParsers.factoryNT)
    RDFParserRegistry.registerLangQuads(RDFLanguages.NQUADS, RiotParsers.factoryNQ)
    initialized = true
  }

  /** Base reader for parsing N-Triples and N-Quads. Heavily inspired by the Jena Riot code:
    * https://github.com/apache/jena/blob/bd97ad4cf731ade857926787dd2df735644a354b/jena-arq/src/main/java/org/apache/jena/riot/lang/RiotParsers.java
    * @param parserProfile
    *   parser profile
    */
  private abstract class BaseReader(parserProfile: ParserProfile) extends ReaderRIOT:
    def create(tokenizer: Tokenizer, output: StreamRDF, context: Context): LangRIOT

    final def read(
        in: InputStream,
        baseURI: String,
        ct: ContentType,
        output: StreamRDF,
        context: Context,
    ): Unit =
      val tok = TokenizerText.create()
        .source(in)
        .errorHandler(parserProfile.getErrorHandler)
        .build()
      create(tok, output, context).parse()

    final def read(
        reader: Reader,
        baseURI: String,
        ct: ContentType,
        output: StreamRDF,
        context: Context,
    ): Unit =
      val tok = TokenizerText.create()
        .source(reader)
        .errorHandler(parserProfile.getErrorHandler)
        .build()
      create(tok, output, context).parse()

  private final class NTriplesReader(parserProfile: ParserProfile)
      extends BaseReader(parserProfile):
    override def create(
        tokenizer: Tokenizer,
        output: StreamRDF,
        context: Context,
    ): LangRIOT = new LangNTriplesGeneralized(tokenizer, parserProfile, output)

  private final class NQuadsReader(parserProfile: ParserProfile) extends BaseReader(parserProfile):
    override def create(
        tokenizer: Tokenizer,
        output: StreamRDF,
        context: Context,
    ): LangRIOT = new LangNQuadsGeneralized(tokenizer, parserProfile, output)
