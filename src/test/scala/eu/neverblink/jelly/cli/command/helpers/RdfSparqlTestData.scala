package eu.neverblink.jelly.cli.command.helpers

import eu.neverblink.jelly.cli.command.rdf.util.{JellyUtil, RdfJellySerializationOptions}
import eu.neverblink.jelly.cli.command.sparql.SparqlSerDesSpec
import eu.neverblink.jelly.cli.util.jena.RdfSparqlConverter
import eu.neverblink.jelly.cli.util.jena.riot.JellyWriterUtil
import eu.neverblink.jelly.convert.jena.JenaConverterFactory
import eu.neverblink.jelly.convert.jena.riot.JellyLanguage
import eu.neverblink.jelly.core.RdfHandler.AnyStatementHandler
import eu.neverblink.jelly.convert.jena.sparql.JellySparqlLanguage
import eu.neverblink.jelly.core.{JellyConstants, JellyOptions}
import eu.neverblink.jelly.core.proto.v1.{PhysicalStreamType, RdfStreamOptions, RdfVersion}
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.query.ResultSetFactory
import org.apache.jena.riot.RDFParser
import org.apache.jena.sparql.core.{DatasetGraphFactory, Quad}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

/** Test data and helpers for converting between Jelly-RDF and Jelly-SPARQL. */
object RdfSparqlTestData:

  def iri(s: String): Node = NodeFactory.createURI("http://example.org/" + s)
  def lit(s: String): Node = NodeFactory.createLiteralString(s)

  val triples: Seq[Triple] = Seq(
    Triple.create(iri("a"), iri("p"), iri("b")),
    Triple.create(iri("a"), iri("q"), lit("hello")),
    Triple.create(iri("b"), iri("p"), NodeFactory.createLiteralLang("cześć", "pl")),
  )

  val quads: Seq[Quad] = Seq(
    Quad.create(iri("g1"), triples(0)),
    Quad.create(iri("g2"), triples(1)),
    Quad.create(Quad.defaultGraphIRI, triples(2)),
  )

  /** The triples as quads in the default graph. */
  val triplesAsQuads: Set[Quad] = triples.map(Quad.create(Quad.defaultGraphIRI, _)).toSet

  /** Writes the statements as Jelly-RDF with the given physical type.
    *
    * @param rowLayout
    *   whether to write Jelly-RDF 1.1, rather than 1.2. GRAPHS streams are always Jelly-RDF 1.1.
    * @param rdfStar
    *   whether triple terms are allowed, in Jelly-RDF 1.1
    * @param rdfVersion
    *   the RDF version, in Jelly-RDF 1.2
    */
  def writeRdf(
      physicalType: PhysicalStreamType,
      triples: Seq[Triple] = Nil,
      quads: Seq[Quad] = Nil,
      rdfStar: Boolean = true,
      rdfVersion: RdfVersion = RdfVersion.RDF_VERSION_UNSPECIFIED,
      rowsPerFrame: Int = 256,
      rowLayout: Boolean = false,
  ): Array[Byte] =
    val out = ByteArrayOutputStream()
    val options =
      if rowLayout || physicalType == PhysicalStreamType.GRAPHS then
        JellyOptions.SMALL_ALL_FEATURES.clone().setRdfStar(rdfStar)
          .setVersion(JellyConstants.PROTO_VERSION_1_1_X)
      else JellyOptions.SMALL_STRICT.clone().setRdfVersion(rdfVersion)
    val writer = JellyWriterUtil.createWriter(
      options.setPhysicalType(physicalType),
      rowsPerFrame,
      false,
      true,
      out,
    )
    writer.start()
    triples.foreach(writer.triple)
    quads.foreach(writer.quad)
    writer.finish()
    out.toByteArray

  /** Reads Jelly-RDF into a set of quads. Triples end up in the default graph. */
  def readRdf(jelly: Array[Byte]): Set[Quad] =
    val dataset = DatasetGraphFactory.create()
    RDFParser.source(ByteArrayInputStream(jelly)).lang(JellyLanguage.JELLY).parse(dataset)
    dataset.find().asScala.map { q =>
      if q.isDefaultGraph then Quad.create(Quad.defaultGraphIRI, q.asTriple) else q
    }.toSet

  /** The stream options of a Jelly-RDF stream. */
  def rdfStreamOptions(jelly: Array[Byte]): RdfStreamOptions =
    JellyUtil.requireFirstOptions(JellyUtil.iterateRdfStream(ByteArrayInputStream(jelly)).buffered)

  /** Converts Jelly-SPARQL to Jelly-RDF, just like rdf to-jelly does with no options. */
  def sparqlToRdfMessages(sparql: Array[Byte]): Array[Byte] =
    val out = ByteArrayOutputStream()
    RdfSparqlConverter.sparqlToRdf(
      ByteArrayInputStream(sparql),
      out,
      RdfJellySerializationOptions.defaultOptions,
      streamTypeSet = false,
      256,
      delimited = true,
    )
    out.toByteArray

  /** Reads a Jelly-RDF stream of RDF Messages into one set of quads per message. */
  def readRdfMessages(jelly: Array[Byte]): Seq[Set[Quad]] =
    val messages = ArrayBuffer[Set[Quad]]()
    var current = Set[Quad]()
    val handler = new AnyStatementHandler[Node]:
      override def handleTriple(s: Node, p: Node, o: Node): Unit =
        current += Quad.create(Quad.defaultGraphIRI, s, p, o)
      override def handleQuad(s: Node, p: Node, o: Node, g: Node): Unit =
        current += Quad.create(if Quad.isDefaultGraph(g) then Quad.defaultGraphIRI else g, s, p, o)
      override def handleMessageEnd(): Unit =
        messages += current
        current = Set()
    val decoder = JenaConverterFactory.getInstance()
      .anyStatementDecoder(handler, JellyOptions.DEFAULT_SUPPORTED_OPTIONS)
    JellyUtil.iterateRdfStream(ByteArrayInputStream(jelly)).foreach(decoder.ingestFrame)
    // The end of the stream ends the last message
    (messages :+ current).toSeq

  /** Reads Jelly-SPARQL into the variables and rows, with each row as a map of the bound values. */
  def readSparql(jelly: Array[Byte]): (Seq[String], Seq[Map[String, Node]]) =
    val rs = ResultSetFactory.makeRewindable(
      SparqlSerDesSpec.parse(jelly, JellySparqlLanguage.JELLY_SPARQL),
    )
    val vars = rs.getResultVars.asScala.toSeq
    val rows = Iterator.continually(rs).takeWhile(_.hasNext).map(_.nextBinding()).map { b =>
      b.vars().asScala.map(v => v.getVarName -> b.get(v)).toMap
    }.toSeq
    (vars, rows)

  /** Writes a SELECT result set with the given variables and rows (JSON terms) as Jelly-SPARQL. */
  def writeSparql(vars: Seq[String], rows: Seq[Map[String, String]]): Array[Byte] =
    SparqlSerDesSpec.writeJelly(sparqlJson(vars, rows))

  /** A SELECT result set with the given variables and rows (JSON terms) as SPARQL JSON. */
  def sparqlJson(vars: Seq[String], rows: Seq[Map[String, String]]): String =
    val bindings = rows.map { row =>
      row.map((k, v) => s"\"$k\": $v").mkString("{ ", ", ", " }")
    }
    s"""{ "head": { "vars": [ ${vars.map(v => s"\"$v\"").mkString(", ")} ] },
       |  "results": { "bindings": [ ${bindings.mkString(", ")} ] } }""".stripMargin

  /** A solution of ?s ?p ?o, and optionally ?g, with IRIs and a literal object. */
  def statementRow(s: String, p: String, o: String, g: Option[String] = None): Map[String, String] =
    Map("s" -> iriJson(s), "p" -> iriJson(p), "o" -> litJson(o)) ++ g.map("g" -> iriJson(_))

  def iriJson(s: String): String = s"""{"type":"uri","value":"http://example.org/$s"}"""
  def litJson(s: String): String = s"""{"type":"literal","value":"$s"}"""
