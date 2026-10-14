package eu.neverblink.jelly.cli.util.jena.riot

import eu.neverblink.jelly.cli.CriticalException
import eu.neverblink.jelly.cli.command.helpers.RdfSparqlTestData.*
import eu.neverblink.jelly.cli.command.rdf.util.JellyUtil
import eu.neverblink.jelly.core.JellyOptions
import eu.neverblink.jelly.core.proto.v1.{PhysicalStreamType, RdfStreamType}
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.Quad
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class JellyMessageWriterSpec extends AnyWordSpec with Matchers:

  private def options(physicalType: PhysicalStreamType) =
    JellyOptions.SMALL_STRICT.clone()
      .setPhysicalType(physicalType)
      .setStreamType(RdfStreamType.MESSAGES)

  /** Triples with all-new IRIs, so that they fill up the name lookup. */
  private def distinctTriples(n: Int): Seq[Triple] =
    (0 until n).map(i => Triple.create(iri(s"s$i"), iri(s"p$i"), iri(s"o$i")))

  "JellyMessageWriter" should {
    "write one message per endMessage call, across frames" in {
      val out = ByteArrayOutputStream()
      val writer = JellyMessageWriter(options(PhysicalStreamType.TRIPLES), 1, true, out)
      writer.triple(triples(0))
      writer.triple(triples(1))
      writer.endMessage()
      writer.endMessage()
      writer.triple(triples(2))
      writer.finish()
      val bytes = out.toByteArray
      JellyUtil.iterateRdfStream(ByteArrayInputStream(bytes)).size should be > 1
      rdfStreamOptions(bytes).getStreamType should be(RdfStreamType.MESSAGES)
      readRdfMessages(bytes) should be(
        Seq(
          Set(triples(0), triples(1)).map(Quad.create(Quad.defaultGraphIRI, _)),
          Set(),
          Set(Quad.create(Quad.defaultGraphIRI, triples(2))),
        ),
      )
    }

    "write quads, with the default graph" in {
      val out = ByteArrayOutputStream()
      val writer = JellyMessageWriter(options(PhysicalStreamType.QUADS), 256, true, out)
      quads.foreach(writer.quad)
      writer.finish()
      readRdfMessages(out.toByteArray) should be(Seq(quads.toSet))
    }

    "refuse a quad in a TRIPLES stream" in {
      val writer = JellyMessageWriter(
        options(PhysicalStreamType.TRIPLES),
        256,
        true,
        ByteArrayOutputStream(),
      )
      val e = intercept[CriticalException](writer.quad(quads.head))
      e.getMessage should include("Cannot write quads to a Jelly TRIPLES stream")
    }

    "write everything in one frame if the output is not delimited" in {
      val out = ByteArrayOutputStream()
      val writer = JellyMessageWriter(options(PhysicalStreamType.TRIPLES), 1, false, out)
      distinctTriples(10).foreach(writer.triple)
      writer.endMessage()
      writer.finish()
      readRdfMessages(out.toByteArray).map(_.size) should be(Seq(10, 0))
    }

    "refuse non-delimited output that does not fit in one frame" in {
      // The name lookup has 128 entries, so 300 triples with new IRIs need more than one frame
      val writer = JellyMessageWriter(
        options(PhysicalStreamType.TRIPLES),
        1,
        false,
        ByteArrayOutputStream(),
      )
      val e = intercept[CriticalException] {
        distinctTriples(300).foreach(writer.triple)
        writer.finish()
      }
      e.getMessage should include("does not fit in a single frame")
    }
  }
