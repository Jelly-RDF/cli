package eu.neverblink.jelly.cli.command.sparql

import com.google.protobuf.ByteString
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import eu.neverblink.jelly.core.proto.v1.sparql.{SparqlResultsFrame, SparqlResultsTrailer}
import eu.neverblink.jelly.core.sparql.JellySparqlOptions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.yaml.snakeyaml.Yaml

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import scala.jdk.CollectionConverters.*

class SparqlInspectSpec extends AnyWordSpec with TestFixtureHelper with Matchers:
  import SparqlSerDesSpec.*

  // Not used by these tests – the SPARQL fixtures are written out by hand.
  protected val testCardinality: Int = 0

  SparqlInspect.testMode(true)

  private type YamlMap = util.Map[String, Any]

  private def inspect(jelly: Array[Byte], args: String*): YamlMap =
    SparqlInspect.setStdIn(ByteArrayInputStream(jelly))
    val (out, _) = SparqlInspect.runTestCommand(List("sparql", "inspect") ++ args)
    Yaml().load(out).asInstanceOf[YamlMap]

  private def perFrame(parsed: YamlMap): Seq[YamlMap] =
    parsed.get("frames").asInstanceOf[util.List[YamlMap]].asScala.toSeq

  private def aggregate(parsed: YamlMap): YamlMap =
    parsed.get("frames").asInstanceOf[YamlMap]

  "sparql inspect command" should {
    "print the stream options, the variables, and aggregate statistics" in {
      val parsed = inspect(writeJelly(selectJson))
      val options = parsed.get("stream_options").asInstanceOf[YamlMap]
      options.get("stream_name") shouldBe ""
      options.get("max_name_table_size") shouldBe JellySparqlOptions.BIG.getMaxNameTableSize
      options.get("version") shouldBe 1
      options.get("rdf_version") shouldBe "RDF_VERSION_UNSPECIFIED (0)"
      parsed.get("variables").asInstanceOf[util.List[String]].asScala shouldBe
        Seq("s", "label", "num", "bn")
      parsed.containsKey("ask_result") shouldBe false
      val frames = aggregate(parsed)
      frames.get("frame_count") shouldBe 1
      frames.get("row_count") shouldBe 3
      frames.get("option_count") shouldBe 1
      frames.get("variable_count") shouldBe 4
      frames.get("iri_value_count") shouldBe 3
      frames.get("bnode_value_count") shouldBe 1
      frames.get("trailer_count") shouldBe 1
      frames.get("ask_result_count") shouldBe 0
      frames.containsKey("trailer_error") shouldBe false
    }

    "print the variables as a plain YAML list" in {
      SparqlInspect.setStdIn(ByteArrayInputStream(writeJelly(selectJson)))
      val (out, _) = SparqlInspect.runTestCommand(List("sparql", "inspect"))
      val nl = System.lineSeparator()
      out should include(
        Seq("variables: ", "  - \"s\"", "  - \"label\"", "  - \"num\"", "  - \"bn\"").mkString(nl),
      )
    }

    "print statistics per frame" in {
      val parsed = inspect(toSmallFrames(selectJson, 4), "--per-frame")
      val frames = perFrame(parsed)
      frames.size should be > 1
      frames.map(_.get("frame_index")) shouldBe frames.indices
      frames.map(_.get("row_count").asInstanceOf[Int]).sum shouldBe 3
      frames.head.get("option_count") shouldBe 1
      frames.tail.map(_.get("option_count")).toSet shouldBe Set(0)
      frames.last.get("trailer_count") shouldBe 1
    }

    "aggregate statistics over several frames" in {
      val jelly = toSmallFrames(selectJson, 4)
      val frames = aggregate(inspect(jelly))
      frames.get("frame_count") shouldBe readFrames(jelly).size
      frames.get("row_count") shouldBe 3
    }

    "report sizes instead of counts with --size" in {
      val jelly = writeJelly(selectJson)
      val frames = aggregate(inspect(jelly, "--size"))
      frames.get("frame_size") shouldBe readFrames(jelly).head.getSerializedSize
      frames.get("row_count") shouldBe 3
      frames.get("iri_value_size").asInstanceOf[Int] should be > 0
      frames.containsKey("iri_value_count") shouldBe false
    }

    "print the result of an ASK query" in {
      for value <- Seq(true, false) do
        val parsed = inspect(writeJelly(askJson(value)))
        parsed.get("ask_result") shouldBe value
        parsed.containsKey("variables") shouldBe false
        aggregate(parsed).get("ask_result_count") shouldBe 1
    }

    "print frame metadata in --per-frame" in {
      val frame = readFrames(writeJelly(selectJson)).head.clone()
      frame.addMetadata(
        SparqlResultsFrame.MetadataEntry.newInstance()
          .setKey("key").setValue(ByteString.copyFromUtf8("abc")),
      )
      frame.resetCachedSize()
      val frames = perFrame(inspect(writeFrames(Seq(frame)), "--per-frame"))
      frames.head.get("metadata").asInstanceOf[YamlMap].get("key") shouldBe "616263"
    }

    "print the error reported by the trailer" in {
      val frame = readFrames(writeJelly(selectJson)).head.clone()
        .setTrailer(SparqlResultsTrailer.newInstance().setError("query timeout"))
      frame.resetCachedSize()
      val jelly = writeFrames(Seq(frame))
      aggregate(inspect(jelly)).get("trailer_error") shouldBe "query timeout"
      perFrame(inspect(jelly, "--per-frame")).head.get("trailer_error") shouldBe "query timeout"
    }

    "print an empty list of variables for a zero-variable result set" in {
      val json = """{ "head": { "vars": [] }, "results": { "bindings": [ {}, {} ] } }"""
      val parsed = inspect(writeJelly(json))
      parsed.get("variables").asInstanceOf[util.List[String]].asScala shouldBe empty
      aggregate(parsed).get("row_count") shouldBe 2
    }

    "complain about empty input" in {
      val e = intercept[ExitException](inspect(Array()))
      e.getCause.getMessage should include("Empty input stream")
    }

    "complain about malformed input" in {
      val e = intercept[ExitException](inspect("definitely not Jelly".getBytes(UTF_8)))
      e.getCause shouldBe a[InvalidJellyFile]
    }

    "complain about a first frame without stream options" in {
      val frames = readFrames(toSmallFrames(selectJson, 4))
      val e = intercept[ExitException](inspect(writeFrames(frames.tail)))
      e.getCause.getMessage should include("does not contain stream options")
    }
  }
