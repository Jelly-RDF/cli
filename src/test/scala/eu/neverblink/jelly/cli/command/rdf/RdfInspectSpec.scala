package eu.neverblink.jelly.cli.command.rdf

import com.google.protobuf.ByteString
import eu.neverblink.jelly.cli.{ExitException, InvalidJellyFile}
import eu.neverblink.jelly.cli.command.helpers.{RdfSparqlTestData, TestFixtureHelper}
import eu.neverblink.jelly.cli.command.sparql.SparqlSerDesSpec
import eu.neverblink.jelly.core.{JellyConstants, JellyOptions}
import eu.neverblink.jelly.cli.util.jena.riot.JellyWriterUtil
import eu.neverblink.jelly.core.proto.v1.{PhysicalStreamType, RdfStreamFrame, RdfStreamRow}
import org.apache.jena.graph.{NodeFactory, Triple}

import scala.jdk.CollectionConverters.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.yaml.snakeyaml.Yaml

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

class RdfInspectSpec extends AnyWordSpec with Matchers with TestFixtureHelper:
  protected val testCardinality: Int = 33
  protected val tripleTerms: Seq[String] = Seq("subject", "predicate", "object")
  protected val tripleNodes: Seq[String] = Seq("iri", "bnode", "literal", "triple")

  /** Most tests here are about Jelly-RDF 1.1 streams, see "for Jelly-RDF 1.2 streams" below. */
  private def withRowLayoutJellyFile(testCode: String => Any, frameSize: Int = 256): Unit =
    withFullJellyFile(testCode, frameSize, rowLayout = true)

  private def inspect(args: String*): util.Map[String, Any] =
    val (out, _) = RdfInspect.runTestCommand(List("rdf", "inspect") ++ args)
    new Yaml().load(out).asInstanceOf[util.Map[String, Any]]

  private def aggregate(parsed: util.Map[String, Any]): util.Map[String, Any] =
    parsed.get("frames").asInstanceOf[util.Map[String, Any]]

  private def perFrame(parsed: util.Map[String, Any]): Seq[util.Map[String, Any]] =
    parsed.get("frames").asInstanceOf[util.List[util.Map[String, Any]]].asScala.toSeq

  private def map(m: util.Map[String, Any], key: String): util.Map[String, Any] =
    m.get(key).asInstanceOf[util.Map[String, Any]]

  "rdf inspect command, for Jelly-RDF 1.2 streams" should {
    "print the stream options of Jelly-RDF 1.2" in withFullJellyFile { j =>
      val options = map(inspect(j), "stream_options")
      options.get("max_name_table_size") should be(128)
      options.get("physical_type") should be("TRIPLES (1)")
      options.get("stream_type") should be("FLAT (0)")
      options.get("rdf_version") should be("RDF_VERSION_UNSPECIFIED (0)")
      options.get("version") should be(JellyConstants.PROTO_VERSION_1_2_X)
      options.containsKey("logical_type") should be(false)
      options.containsKey("generalized_statements") should be(false)
    }

    "count statements, lookup entries, and column values" in withFullJellyFile(
      j => {
        val frames = aggregate(inspect(j))
        frames.get("frame_count") should be(3)
        frames.get("statement_count") should be(testCardinality)
        frames.get("option_count") should be(1)
        frames.get("name_count").asInstanceOf[Int] should be > 0
        // Subject, predicate, and object are all IRIs
        frames.get("iri_value_count") should be(3 * testCardinality)
        frames.get("literal_value_count") should be(0)
        frames.containsKey("triple_count") should be(false)
        frames.containsKey("message_end_count") should be(false)
      },
      frameSize = 15,
    )

    "count statements per frame" in withFullJellyFile(
      j => {
        val frames = perFrame(inspect("--per-frame", j))
        frames.map(_.get("statement_count")) should be(Seq(15, 15, 3))
        frames.map(_.get("frame_index")) should be(Seq(0, 1, 2))
      },
      frameSize = 15,
    )

    "print the values of each column with --detail all" in withFullJellyFile(
      j => {
        val frames = aggregate(inspect("--detail", "all", j))
        for column <- tripleTerms do
          map(frames, column).get("iri_value_count") should be(testCardinality)
          map(frames, column).containsKey("bnode_value_count") should be(false)
        frames.containsKey("graph") should be(false)
        val first = perFrame(inspect("--per-frame", "--detail", "all", j)).head
        map(first, "subject").get("iri_value_count") should be(15)
      },
      frameSize = 15,
    )

    "print the values by kind with --detail node" in withFullJellyFile { j =>
      val details = map(aggregate(inspect("--detail", "node", j)), "node_details")
      details.get("iri_value_count") should be(3 * testCardinality)
    }

    "print the values by column with --detail term" in withFullJellyFile { j =>
      val details = map(aggregate(inspect("--detail", "term", j)), "term_details")
      details.get("subject_count") should be(testCardinality)
      details.get("graph_count") should be(0)
    }

    "print size statistics" in withFullJellyFile { j =>
      val frames = aggregate(inspect("--size", "--detail", "all", j))
      // The number of statements is not a size
      frames.get("statement_count") should be(testCardinality)
      frames.get("frame_size").asInstanceOf[Int] should be > testCardinality
      frames.get("iri_value_size").asInstanceOf[Int] should be > 0
      map(frames, "subject").get("iri_value_size").asInstanceOf[Int] should be > 0
    }

    "print the values of the graph column of a QUADS stream" in {
      import RdfSparqlTestData.*
      RdfInspect.setStdIn(ByteArrayInputStream(writeRdf(PhysicalStreamType.QUADS, quads = quads)))
      val parsed = inspect("--detail", "all")
      map(parsed, "stream_options").get("physical_type") should be("QUADS (2)")
      val frames = aggregate(parsed)
      frames.get("statement_count") should be(quads.size)
      // Two named graphs, and one quad in the default graph, which has no value
      map(frames, "graph").get("iri_value_count") should be(2)
      RdfInspect.setStdIn(ByteArrayInputStream(writeRdf(PhysicalStreamType.QUADS, quads = quads)))
      map(aggregate(inspect("--detail", "term")), "term_details").get("graph_count") should be(2)
    }

    "measure the size of triple terms" in {
      import RdfSparqlTestData.*
      val withTripleTerm = Triple.create(
        iri("a"),
        iri("says"),
        NodeFactory.createTripleTerm(triples.head),
      )
      val rdf = writeRdf(PhysicalStreamType.TRIPLES, Seq(withTripleTerm))
      RdfInspect.setStdIn(ByteArrayInputStream(rdf))
      aggregate(inspect()).get("triple_term_value_count") should be(1)
      RdfInspect.setStdIn(ByteArrayInputStream(rdf))
      val frames = aggregate(inspect("--size", "--detail", "all"))
      frames.get("triple_term_value_size").asInstanceOf[Int] should be > 0
      map(frames, "object").get("triple_term_value_size") should be(
        frames.get("triple_term_value_size"),
      )
    }

    "count namespace declarations" in {
      val out = ByteArrayOutputStream()
      val writer = JellyWriterUtil.createWriter(
        JellyOptions.SMALL_STRICT.clone().setPhysicalType(PhysicalStreamType.TRIPLES),
        256,
        enableNamespaceDeclarations = true,
        delimited = true,
        out,
      )
      writer.prefix("ex", "http://example.org/")
      writer.prefix("foaf", "http://xmlns.com/foaf/0.1/")
      writer.triple(RdfSparqlTestData.triples.head)
      writer.finish()
      RdfInspect.setStdIn(ByteArrayInputStream(out.toByteArray))
      aggregate(inspect()).get("namespace_count") should be(2)
      RdfInspect.setStdIn(ByteArrayInputStream(out.toByteArray))
      aggregate(inspect("--size")).get("namespace_size").asInstanceOf[Int] should be > 0
    }

    "count the message ends of a stream of RDF Messages" in {
      import RdfSparqlTestData.*
      val result = sparqlJson(Seq("s", "p", "o"), Seq(statementRow("a", "p", "1")))
      val sparql = SparqlSerDesSpec.writePunctuated(Seq(result, result, result))
      RdfInspect.setStdIn(ByteArrayInputStream(sparqlToRdfMessages(sparql)))
      val parsed = inspect()
      map(parsed, "stream_options").get("stream_type") should be("MESSAGES (1)")
      // The end of the stream ends the last message
      aggregate(parsed).get("message_end_count") should be(2)
      aggregate(parsed).get("statement_count") should be(3)
    }
  }

  "rdf inspect command" should {
    "be able to return aggregate of all frames as a valid Yaml" in withRowLayoutJellyFile { j =>
      val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", j))
      val yaml = new Yaml()
      val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
      parsed.get("stream_options") should not be None
      val options = parsed.get("stream_options").asInstanceOf[java.util.Map[String, Any]]
      options.get("max_name_table_size") should be(128)
      parsed.get("frames") shouldBe a[util.LinkedHashMap[?, ?]]
      val frames = parsed.get("frames").asInstanceOf[java.util.LinkedHashMap[String, Any]]
      frames.get("triple_count") should be(testCardinality)
    }

    "be able to return all frames separately as a valid Yaml" in withRowLayoutJellyFile(
      testCode = { j =>
        val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--per-frame", j))
        val yaml = new Yaml()
        val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
        parsed.get("stream_options") should not be None
        parsed.get("frames") shouldBe a[util.ArrayList[Map[String, Int]]]
        val frames =
          parsed.get("frames").asInstanceOf[util.ArrayList[util.HashMap[String, Int]]].asScala
        frames.length should be <= 5
        frames.map(_.get("triple_count")).sum should be(testCardinality)
      },
      frameSize = 15,
    )

    "handle properly separate frame metrics for a singular frame" in withRowLayoutJellyFile { j =>
      val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--per-frame", j))
      val yaml = new Yaml()
      val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
      parsed.get("stream_options") should not be None
      parsed.get("frames") shouldBe a[util.ArrayList[?]]
      val frames =
        parsed.get("frames").asInstanceOf[util.ArrayList[util.HashMap[String, Int]]].asScala
      frames.length should be(1)
      frames.map(_.get("triple_count")).sum should be(testCardinality)
    }

    "handle properly frame count when aggregating multiple frames" in withRowLayoutJellyFile(
      testCode = { j =>
        val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", j))
        val yaml = new Yaml()
        val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
        parsed.get("stream_options") should not be None
        val options = parsed.get("stream_options").asInstanceOf[java.util.Map[String, Any]]
        options.get("max_name_table_size") should be(128)
        parsed.get("frames") shouldBe a[util.LinkedHashMap[?, ?]]
        val frames = parsed.get("frames").asInstanceOf[java.util.LinkedHashMap[String, Any]]
        frames.get("triple_count") should be(testCardinality)
        frames.get("frame_count") should be(5)
      },
      frameSize = 15,
    )

    "print frame metadata in --per-frame" in {
      val inFrame = RdfStreamFrame.newInstance()
        .addRows(
          RdfStreamRow.newInstance()
            .setOptions(JellyOptions.BIG_GENERALIZED),
        )
        .addMetadata(
          RdfStreamFrame.MetadataEntry.newInstance()
            .setKey("key")
            .setValue(ByteString.fromHex("1337ff")),
        )
      val inBytes = inFrame.toByteArray
      RdfInspect.setStdIn(ByteArrayInputStream(inBytes))
      val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--per-frame"))
      val yaml = new Yaml()
      val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
      val frame0 =
        parsed.get("frames").asInstanceOf[util.ArrayList[util.HashMap[String, Any]]].get(0)
      frame0.get("frame_index") should be(0)
      frame0.get("metadata") should not be None
      val metadata = frame0.get("metadata").asInstanceOf[util.HashMap[String, String]]
      metadata.get("key") should be("1337ff")
    }

    "throw an error if the input file is not a valid Jelly file" in withEmptyJellyFile { j =>
      val exception = intercept[ExitException] {
        RdfInspect.runTestCommand(List("rdf", "inspect", j, "--debug"))
      }
      val msg = InvalidJellyFile(RuntimeException("")).getMessage
      exception.getMessage should include(msg)
    }

    "print detailed metrics" when {
      "aggregating and flat" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--detail", "all", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[String, util.LinkedHashMap[String, ?]]]
          val frames = parsed.get("frames").asInstanceOf[
            util.LinkedHashMap[String, util.LinkedHashMap[String, Any]],
          ]
          frames.get("subject").get("iri_count") shouldBe testCardinality
          frames.get("subject").get("bnode_count") == null shouldBe true
        },
        frameSize = 15,
      )

      "per frame and flat" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--per-frame", "--detail", "all", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.ArrayList[?]]
          val frames = parsed.get("frames").asInstanceOf[util.ArrayList[Any]]
          frames.get(0) shouldBe a[util.LinkedHashMap[String, util.LinkedHashMap[String, ?]]]
          val frame1 =
            frames.get(0).asInstanceOf[util.LinkedHashMap[String, util.LinkedHashMap[String, Any]]]
          frame1.get("subject").get("iri_count") shouldBe 6
          frame1.get("subject").get("bnode_count") == null shouldBe true
        },
        frameSize = 15,
      )

      "aggregating and grouping by node" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--detail", "node", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[?, ?]]
          val frames = parsed.get("frames").asInstanceOf[util.LinkedHashMap[String, Any]]
          frames.get("node_details") shouldBe a[util.LinkedHashMap[?, ?]]
          val nodeDetails = frames.get("node_details").asInstanceOf[util.LinkedHashMap[String, Any]]
          nodeDetails.get("iri_count") shouldBe testCardinality * 3
          nodeDetails.get("bnode_count") == null shouldBe true
        },
        frameSize = 15,
      )

      "per frame and grouping by node" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--per-frame", "--detail", "node", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.ArrayList[?]]
          val frames = parsed.get("frames").asInstanceOf[util.ArrayList[Any]]
          frames.get(0) shouldBe a[util.LinkedHashMap[?, ?]]
          val frame1 = frames.get(0).asInstanceOf[util.Map[String, Any]]
          val nodeDetails = frame1.get("node_details").asInstanceOf[util.LinkedHashMap[String, Any]]
          nodeDetails.get("iri_count") shouldBe 6 * 3
          nodeDetails.get("bnode_count") == null shouldBe true
        },
        frameSize = 15,
      )

      "aggregating and grouping by term" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--detail", "term", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[?, ?]]
          val frames = parsed.get("frames").asInstanceOf[util.LinkedHashMap[String, Any]]
          frames.get("term_details") shouldBe a[util.LinkedHashMap[?, ?]]
          val nodeDetails = frames.get("term_details").asInstanceOf[util.LinkedHashMap[String, Any]]
          nodeDetails.get("subject_count") shouldBe testCardinality
        },
        frameSize = 15,
      )

      "per frame and grouping by term" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--per-frame", "--detail", "term", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.ArrayList[?]]
          val frames = parsed.get("frames").asInstanceOf[util.ArrayList[Any]]
          frames.get(0) shouldBe a[util.LinkedHashMap[?, ?]]
          val frame1 = frames.get(0).asInstanceOf[util.Map[String, Any]]
          val nodeDetails = frame1.get("term_details").asInstanceOf[util.LinkedHashMap[String, Any]]
          nodeDetails.get("subject_count") shouldBe 6
        },
        frameSize = 15,
      )

      "given complex jelly file (triples)" in withSpecificJellyFile(
        testCode = { jellyF =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--detail", "all", jellyF))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[String, util.LinkedHashMap[String, ?]]]
          val frames = parsed.get("frames").asInstanceOf[
            util.LinkedHashMap[String, util.LinkedHashMap[String, Any]],
          ]
          for
            term <- tripleTerms
            node <- tripleNodes
          do frames.get(term).get(s"${node}_count").asInstanceOf[Int] should be > 0

          // Graphs == 0 when doing triples
          for node <- "default_graph" +: tripleNodes do frames.get("graph") == null shouldBe true
          // These are illegal
          for term <- tripleTerms do
            frames.get(term).get("default_graph_count") == null shouldBe true
        },
        fileName = "everythingTriple.jelly",
      )

      "given complex jelly file (quads)" in withSpecificJellyFile(
        testCode = { jellyF =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--detail", "all", jellyF))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
          parsed.get("frames") shouldBe a[
            util.LinkedHashMap[String, java.util.LinkedHashMap[String, ?]],
          ]
          val frames = parsed.get("frames").asInstanceOf[
            java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Any]],
          ]
          for
            term <- tripleTerms
            node <- tripleNodes
          do frames.get(term).get(s"${node}_count").asInstanceOf[Int] should be > 0
          for node <- Seq("iri", "bnode", "literal", "default_graph") do
            frames.get("graph").get(s"${node}_count").asInstanceOf[Int] should be > 0

          // These are illegal
          for term <- tripleTerms do
            frames.get(term).get("default_graph_count") == null shouldBe true
          for term <- tripleTerms do frames.get("graph").get("triple_count") == null shouldBe true
        },
        fileName = "everythingQuad.jelly",
      )
    }

    "print size statistics" when {
      "aggregating multiple frames" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--size", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
          parsed.get("stream_options") should not be None
          val options = parsed.get("stream_options").asInstanceOf[java.util.Map[String, Any]]
          options.get("max_name_table_size") should be(128)
          parsed.get("frames") shouldBe a[util.LinkedHashMap[?, ?]]
          val frames = parsed.get("frames").asInstanceOf[java.util.LinkedHashMap[String, Any]]
          frames.get("triple_size") should be(16 * testCardinality)
          frames.get("frame_count") should be(5)
        },
        frameSize = 15,
      )

      "per frame" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--per-frame", "--size", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.ArrayList[?]]
          val frames = parsed.get("frames").asInstanceOf[util.ArrayList[Any]]
          frames.get(0) shouldBe a[util.LinkedHashMap[String, util.LinkedHashMap[String, ?]]]
          val frame1 =
            frames.get(0).asInstanceOf[util.LinkedHashMap[String, Any]]
          frame1 shouldBe a[util.LinkedHashMap[?, ?]]
          frame1.get("triple_size") should be(16 * 6)
        },
        frameSize = 15,
      )
    }

    "print detailed size statistics" when {
      "aggregating multiple frames" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--size", "--detail", "all", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[String, util.LinkedHashMap[String, ?]]]
          val frames = parsed.get("frames").asInstanceOf[
            util.LinkedHashMap[String, util.LinkedHashMap[String, Any]],
          ]
          frames.get("subject").get("iri_size").asInstanceOf[Int] should be > testCardinality
          frames.get("subject").get("bnode_size") == null shouldBe true
        },
        frameSize = 15,
      )

      "per frame" in withRowLayoutJellyFile(
        testCode = { j =>
          val (out, err) =
            RdfInspect.runTestCommand(
              List("rdf", "inspect", "--size", "--per-frame", "--detail", "all", j),
            )
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.ArrayList[?]]
          val frames = parsed.get("frames").asInstanceOf[util.ArrayList[Any]]
          frames.get(0) shouldBe a[util.LinkedHashMap[String, util.LinkedHashMap[String, ?]]]
          val frame1 =
            frames.get(0).asInstanceOf[util.LinkedHashMap[String, util.LinkedHashMap[String, Any]]]
          frame1.get("subject").get("iri_size").asInstanceOf[Int] should be > 6
          frame1.get("subject").get("iri_size").asInstanceOf[Int] should be < 15
          frame1.get("subject").get("bnode_size") == null shouldBe true
        },
        frameSize = 15,
      )

      "given complex jelly file (triples)" in withSpecificJellyFile(
        testCode = { jellyF =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--size", "--detail", "all", jellyF))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[String, util.LinkedHashMap[String, ?]]]
          val frames = parsed.get("frames").asInstanceOf[
            util.LinkedHashMap[String, util.LinkedHashMap[String, Any]],
          ]
          for
            term <- tripleTerms
            node <- tripleNodes
          do frames.get(term).get(s"${node}_size").asInstanceOf[Int] should be > 0

          // Graphs == 0 when doing triples
          for node <- "default_graph" +: tripleNodes do frames.get("graph") == null shouldBe true
          // These are illegal
          for term <- tripleTerms do
            frames.get(term).get("default_graph_size") == null shouldBe true
        },
        fileName = "everythingTriple.jelly",
      )

      "given complex jelly file (quads)" in withSpecificJellyFile(
        testCode = { jellyF =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--size", "--detail", "all", jellyF))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
          parsed.get("frames") shouldBe a[
            util.LinkedHashMap[String, java.util.LinkedHashMap[String, ?]],
          ]
          val frames = parsed.get("frames").asInstanceOf[
            java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Any]],
          ]
          for
            term <- tripleTerms
            node <- tripleNodes
          do frames.get(term).get(s"${node}_size").asInstanceOf[Int] should be > 0
          for node <- Seq("iri", "bnode", "literal") do
            frames.get("graph").get(s"${node}_size").asInstanceOf[Int] should be > 0

          // Default graph is 0 bytes, so it gets omitted in size statistics
          frames.get("graph").get("default_graph_size") == null shouldBe true
          // These are illegal
          for term <- tripleTerms do
            frames.get(term).get("default_graph_size") == null shouldBe true
          for term <- tripleTerms do frames.get("graph").get("triple_size") == null shouldBe true
        },
        fileName = "everythingQuad.jelly",
      )
    }
  }
