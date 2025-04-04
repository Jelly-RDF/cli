package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.{ExitException, InvalidJellyFile}
import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper

import scala.jdk.CollectionConverters.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.yaml.snakeyaml.Yaml

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
    "throw an error if the input file is not a valid jelly file" in withEmptyJellyFile { j =>
      val exception = intercept[ExitException] {
        RdfInspect.runTestCommand(List("rdf", "inspect", j, "--debug"))
      }
      val err = RdfInspect.getErrString
      val msg = InvalidJellyFile(RuntimeException("")).getMessage
      err should include(msg)
    }
  }
