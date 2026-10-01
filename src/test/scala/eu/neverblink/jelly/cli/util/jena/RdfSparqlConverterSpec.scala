package eu.neverblink.jelly.cli.util.jena

import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.helpers.RdfSparqlTestData.*
import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import eu.neverblink.jelly.cli.command.rdf.util.{JellyUtil, RdfJellySerializationOptions}
import eu.neverblink.jelly.cli.command.sparql.SparqlSerDesSpec.*
import eu.neverblink.jelly.core.proto.v1.{PhysicalStreamType, RdfStreamOptions, RdfVersion}
import eu.neverblink.jelly.core.proto.v1.sparql.SparqlResultsOptions
import eu.neverblink.jelly.core.sparql.{JellySparqlConstants, JellySparqlOptions}
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.sparql.core.Quad
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8

class RdfSparqlConverterSpec extends AnyWordSpec with TestFixtureHelper with Matchers:

  // Not used by these tests – the fixtures are written out by hand.
  protected val testCardinality: Int = 0

  private def toSparql(
      jelly: Array[Byte],
      outputOptions: RdfStreamOptions => SparqlResultsOptions =
        RdfSparqlConverter.defaultSparqlOptions,
      valuesPerFrame: Int = JellySparqlConstants.DEFAULT_MAX_VALUES_PER_FRAME,
  ): Array[Byte] =
    val out = ByteArrayOutputStream()
    RdfSparqlConverter.rdfToSparql(
      ByteArrayInputStream(jelly),
      out,
      outputOptions,
      valuesPerFrame,
      delimited = true,
    )
    out.toByteArray

  /** The options the CLI uses when no --opt.* option is given. */
  private def defaultRdfOptions = RdfJellySerializationOptions().asRdfStreamOptions.clone()

  private def toRdf(
      jelly: Array[Byte],
      options: RdfStreamOptions = defaultRdfOptions,
  ): Array[Byte] =
    val out = ByteArrayOutputStream()
    RdfSparqlConverter.sparqlToRdf(ByteArrayInputStream(jelly), out, options, 256, true)
    out.toByteArray

  "rdfToSparql" should {
    "turn a TRIPLES stream into ?s ?p ?o solutions" in {
      val (vars, rows) = readSparql(toSparql(writeRdf(PhysicalStreamType.TRIPLES, triples)))
      vars shouldBe Seq("s", "p", "o")
      rows shouldBe triples.map { t =>
        Map("s" -> t.getSubject, "p" -> t.getPredicate, "o" -> t.getObject)
      }
    }

    "turn a QUADS stream into ?s ?p ?o ?g solutions, with ?g unbound for the default graph" in {
      val (vars, rows) = readSparql(toSparql(writeRdf(PhysicalStreamType.QUADS, quads = quads)))
      vars shouldBe Seq("s", "p", "o", "g")
      rows shouldBe quads.map { q =>
        Map("s" -> q.getSubject, "p" -> q.getPredicate, "o" -> q.getObject) ++
          (if q.isDefaultGraph then Map() else Map("g" -> q.getGraph))
      }
    }

    "leave ?g unbound only for the default graph, wherever it is in the stream" in {
      // The default graph between named graphs, twice, and a named graph repeated after it
      val mixed = Seq(
        Quad.create(iri("g1"), triples(0)),
        Quad.create(Quad.defaultGraphIRI, triples(1)),
        Quad.create(iri("g2"), triples(2)),
        Quad.create(Quad.defaultGraphIRI, triples(0)),
        Quad.create(iri("g1"), triples(1)),
      )
      val expected = mixed.map { q =>
        Map("s" -> q.getSubject, "p" -> q.getPredicate, "o" -> q.getObject) ++
          (if q.isDefaultGraph then Map() else Map("g" -> q.getGraph))
      }
      for
        physicalType <- Seq(PhysicalStreamType.QUADS, PhysicalStreamType.GRAPHS)
        // With 1 row per frame, the graph changes also happen across frame boundaries
        rowsPerFrame <- Seq(256, 1)
      do
        withClue(s"$physicalType, $rowsPerFrame rows per frame: ") {
          val rdf = writeRdf(physicalType, quads = mixed, rowsPerFrame = rowsPerFrame)
          if rowsPerFrame == 1 then
            JellyUtil.iterateRdfStream(ByteArrayInputStream(rdf)).size should be > 1
          val sparql = toSparql(rdf)
          val (vars, rows) = readSparql(sparql)
          vars shouldBe Seq("s", "p", "o", "g")
          rows shouldBe expected
          // And back: the unbound ?g must go to the default graph again
          readRdf(toRdf(sparql)) shouldBe mixed.toSet
        }
    }

    "turn a GRAPHS stream into ?s ?p ?o ?g solutions" in {
      val (vars, rows) = readSparql(toSparql(writeRdf(PhysicalStreamType.GRAPHS, quads = quads)))
      vars shouldBe Seq("s", "p", "o", "g")
      rows.size shouldBe quads.size
      rows.flatMap(_.get("g")).toSet shouldBe Set(iri("g1"), iri("g2"))
    }

    "announce the RDF version based on the input options by default" in {
      for (tripleTerms, version) <- Seq(
          false -> RdfVersion.RDF_VERSION_1_1,
          true -> RdfVersion.RDF_VERSION_1_2,
        )
      do
        val jelly = toSparql(writeRdf(PhysicalStreamType.TRIPLES, triples, rdfStar = tripleTerms))
        val options = readFrames(jelly).head.getOptions
        options.getRdfVersion shouldBe version
        options.getMaxNameTableSize shouldBe JellySparqlOptions.BIG.getMaxNameTableSize
    }

    "use the output options it is given" in {
      val jelly = toSparql(
        writeRdf(PhysicalStreamType.TRIPLES, triples),
        _ => JellySparqlOptions.SMALL.clone().setStreamName("given"),
      )
      val options = readFrames(jelly).head.getOptions
      options.getStreamName shouldBe "given"
      options.getMaxNameTableSize shouldBe JellySparqlOptions.SMALL.getMaxNameTableSize
    }

    "keep triple terms" in {
      val tripleTerm = NodeFactory.createTripleTerm(triples(0))
      val jelly = toSparql(
        writeRdf(PhysicalStreamType.TRIPLES, Seq(Triple.create(iri("a"), iri("says"), tripleTerm))),
      )
      readSparql(jelly)._2.head("o") shouldBe tripleTerm
    }

    "split the output into frames" in {
      val frames = readFrames(
        toSparql(writeRdf(PhysicalStreamType.TRIPLES, triples), valuesPerFrame = 3),
      )
      frames.map(_.getRowCount).sum shouldBe triples.size
      frames.size should be > 1
    }

    "complain about empty input" in {
      // The Jelly writer writes nothing at all when there are no statements
      writeRdf(PhysicalStreamType.TRIPLES) shouldBe empty
      val e = intercept[CriticalException](toSparql(Array()))
      e.getMessage should include("Empty input stream")
    }

    "complain about malformed input" in {
      intercept[InvalidJellyFile](toSparql("not Jelly".getBytes(UTF_8)))
    }
  }

  "sparqlToRdf" should {
    "turn 3 variables into a TRIPLES stream" in {
      val jelly = toRdf(toSparql(writeRdf(PhysicalStreamType.TRIPLES, triples)))
      rdfStreamOptions(jelly).getPhysicalType shouldBe PhysicalStreamType.TRIPLES
      readRdf(jelly) shouldBe triplesAsQuads
    }

    "turn 4 variables into a QUADS stream, with unbound graphs in the default graph" in {
      val jelly = toRdf(toSparql(writeRdf(PhysicalStreamType.QUADS, quads = quads)))
      rdfStreamOptions(jelly).getPhysicalType shouldBe PhysicalStreamType.QUADS
      readRdf(jelly) shouldBe quads.toSet
    }

    "take the variables by position, not by name" in {
      val jelly = toRdf(
        writeSparql(
          Seq("x", "y", "z"),
          Seq(Map("x" -> iriJson("a"), "y" -> iriJson("p"), "z" -> litJson("v"))),
        ),
      )
      readRdf(jelly) shouldBe Set(Quad.create(Quad.defaultGraphIRI, iri("a"), iri("p"), lit("v")))
    }

    "keep the physical type it is given" in {
      val quadsSparql = toSparql(writeRdf(PhysicalStreamType.QUADS, quads = quads))
      val graphs =
        toRdf(quadsSparql, defaultRdfOptions.setPhysicalType(PhysicalStreamType.GRAPHS))
      rdfStreamOptions(graphs).getPhysicalType shouldBe PhysicalStreamType.GRAPHS
      readRdf(graphs) shouldBe quads.toSet
      // 3 variables in a QUADS stream end up in the default graph
      val triplesSparql = toSparql(writeRdf(PhysicalStreamType.TRIPLES, triples))
      val quadsRdf =
        toRdf(triplesSparql, defaultRdfOptions.setPhysicalType(PhysicalStreamType.QUADS))
      rdfStreamOptions(quadsRdf).getPhysicalType shouldBe PhysicalStreamType.QUADS
      readRdf(quadsRdf) shouldBe triplesAsQuads
    }

    "refuse to write 4 variables as a TRIPLES stream" in {
      val sparql = toSparql(writeRdf(PhysicalStreamType.QUADS, quads = quads))
      intercept[InvalidArgument](
        toRdf(sparql, defaultRdfOptions.setPhysicalType(PhysicalStreamType.TRIPLES)),
      )
    }

    "reject a result set without 3 or 4 variables" in {
      for vars <- Seq(Seq("a", "b"), Seq("a", "b", "c", "d", "e")) do
        val e = intercept[CriticalException](toRdf(writeSparql(vars, Nil)))
        e.getMessage should include("must have 3 or 4 variables")
    }

    "reject an ASK result" in {
      val e = intercept[CriticalException](toRdf(writeJelly(askJson(true))))
      e.getMessage should include("ASK result")
    }

    "reject a solution with an unbound subject, predicate, or object" in {
      val sparql = writeSparql(
        Seq("s", "p", "o"),
        Seq(Map("s" -> iriJson("a"), "o" -> litJson("v"))),
      )
      val e = intercept[CriticalException](toRdf(sparql))
      e.getMessage should include("Solution 0 has no value for ?p")
    }

    "reject generalized statements, unless they are enabled" in {
      val sparql = writeSparql(
        Seq("s", "p", "o"),
        Seq(Map("s" -> litJson("lit"), "p" -> iriJson("p"), "o" -> litJson("v"))),
      )
      val e = intercept[CriticalException](toRdf(sparql))
      e.getMessage should include("--opt.generalized-statements=true")
      val jelly = toRdf(sparql, defaultRdfOptions.setGeneralizedStatements(true))
      rdfStreamOptions(jelly).getGeneralizedStatements shouldBe true
    }

    "reject a literal graph name, unless generalized statements are enabled" in {
      val sparql = writeSparql(
        Seq("s", "p", "o", "g"),
        Seq(
          Map("s" -> iriJson("a"), "p" -> iriJson("p"), "o" -> litJson("v"), "g" -> litJson("g")),
        ),
      )
      val e = intercept[CriticalException](toRdf(sparql))
      e.getMessage should include("--opt.generalized-statements=true")
    }

    "reject triple terms, unless they are enabled" in {
      val tripleTerm = NodeFactory.createTripleTerm(triples(0))
      val sparql = toSparql(
        writeRdf(PhysicalStreamType.TRIPLES, Seq(Triple.create(iri("a"), iri("says"), tripleTerm))),
      )
      val e = intercept[CriticalException](toRdf(sparql, defaultRdfOptions.setRdfStar(false)))
      e.getMessage should include("--opt.triple-terms=true")
    }

    "reject a malformed input" in {
      intercept[InvalidJellyFile](toRdf("not Jelly".getBytes(UTF_8)))
    }
  }
