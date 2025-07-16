package eu.neverblink.jelly.cli.command.rdf

import com.google.protobuf.ByteString
import eu.neverblink.jelly.cli.{ExitException, InvalidJellyFile}
import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import eu.neverblink.jelly.core.JellyOptions
import eu.neverblink.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamRow}

import scala.jdk.CollectionConverters.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.yaml.snakeyaml.Yaml

import java.io.ByteArrayInputStream
import java.util

class RdfInspectSpec extends AnyWordSpec with Matchers with TestFixtureHelper:
  protected val testCardinality: Int = 33

  "rdf inspect command" should {
    "be able to return aggregate of all frames as a valid Yaml" in withFullJellyFile { j =>
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

    "be able to return all frames separately as a valid Yaml" in withFullJellyFile(
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

    "handle properly separate frame metrics for a singular frame" in withFullJellyFile { j =>
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

    "handle properly frame count when aggregating multiple frames" in withFullJellyFile(
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
      "aggregating and flat" in withFullJellyFile(
        testCode = { j =>
          val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--detail", "flat", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[?, ?]]
          val frames = parsed.get("frames").asInstanceOf[java.util.LinkedHashMap[String, Any]]
          frames.get("subject_iri_count") shouldBe testCardinality
          frames.get("subject_bnode_count") shouldBe 0
        },
        frameSize = 15,
      )

      "per frame and flat" in withFullJellyFile(
        testCode = { j =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--per-frame", "--detail", "flat", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.ArrayList[?]]
          val frames = parsed.get("frames").asInstanceOf[util.ArrayList[Any]]
          frames.get(0) shouldBe a[util.LinkedHashMap[?, ?]]
          val frame1 = frames.get(0).asInstanceOf[util.Map[String, Any]]
          frame1.get("subject_iri_count") shouldBe 6
          frame1.get("subject_bnode_count") shouldBe 0
        },
        frameSize = 15,
      )

      "aggregating and grouping by node" in withFullJellyFile(
        testCode = { j =>
          val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--detail", "node", j))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[?, ?]]
          val frames = parsed.get("frames").asInstanceOf[util.LinkedHashMap[String, Any]]
          frames.get("node_details") shouldBe a[util.LinkedHashMap[?, ?]]
          val nodeDetails = frames.get("node_details").asInstanceOf[util.LinkedHashMap[String, Any]]
          nodeDetails.get("iri_count") shouldBe testCardinality * 3
          nodeDetails.get("bnode_count") shouldBe 0
        },
        frameSize = 15,
      )

      "per frame and grouping by node" in withFullJellyFile(
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
          nodeDetails.get("bnode_count") shouldBe 0
        },
        frameSize = 15,
      )

      "aggregating and grouping by term" in withFullJellyFile(
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

      "per frame and grouping by term" in withFullJellyFile(
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

      val tripleTerms = Seq("subject", "predicate", "object")
      val tripleNodes = Seq("iri", "bnode", "literal", "triple")

      "given complex jelly file (triples)" in withSpecificJellyFile(
        testCode = { jellyF =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--detail", "flat", jellyF))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[?, ?]]
          val frames = parsed.get("frames").asInstanceOf[java.util.LinkedHashMap[String, Any]]
          for
            term <- tripleTerms
            node <- tripleNodes
          do frames.get(s"${term}_${node}_count") should not be 0

          // Graphs == 0 when doing triples
          for node <- tripleNodes do frames.get(s"graph_${node}_count") shouldBe 0
          // These are illegal
          for term <- tripleTerms do frames.get(s"${term}_default_graph_count") shouldBe 0
        },
        fileName = "everythingTriple.jelly",
      )

      "given complex jelly file (quads)" in withSpecificJellyFile(
        testCode = { jellyF =>
          val (out, err) =
            RdfInspect.runTestCommand(List("rdf", "inspect", "--detail", "flat", jellyF))
          val yaml = new Yaml()
          val parsed = yaml.load(out).asInstanceOf[java.util.Map[String, Any]]
          parsed.get("frames") shouldBe a[util.LinkedHashMap[?, ?]]
          val frames = parsed.get("frames").asInstanceOf[java.util.LinkedHashMap[String, Any]]
          for
            term <- tripleTerms
            node <- tripleNodes
          do frames.get(s"${term}_${node}_count") should not be 0
          for node <- Seq("iri", "bnode", "literal", "default_graph") do
            frames.get(s"graph_${node}_count") should not be 0

          // These are illegal
          for term <- tripleTerms do frames.get(s"${term}_default_graph_count") shouldBe 0
          for term <- tripleTerms do frames.get("graph_triple_count") shouldBe 0
        },
        fileName = "everythingQuad.jelly",
      )
    }
  }
