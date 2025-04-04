package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.yaml.snakeyaml.Yaml

class RdfInspectSpec extends AnyWordSpec with Matchers with TestFixtureHelper:
  protected val testCardinality: Int = 33

  "rdf inspect command" should {
    "be able to return aggregate of all frames as a valid Yaml" in withFullJellyFile { j =>
      val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", j))
      val yaml = new Yaml()
      try {
        yaml.load(out)
        true
      } catch {
        case e: Exception =>
          fail("Failed to parse YAML output", e)
      }
    }
    "be able to return all frames separately as a valid Yaml" in withFullJellyFile(
      testCode = { j =>
        val (out, err) = RdfInspect.runTestCommand(List("rdf", "inspect", "--per-frame", j))
        val yaml = new Yaml()
        try {
          yaml.load(out)
          true
        } catch {
          case e: Exception =>
            fail("Failed to parse YAML output", e)
        }
      },
      frameSize = 15,
    )
  }
