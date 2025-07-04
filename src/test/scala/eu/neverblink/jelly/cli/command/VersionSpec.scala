package eu.neverblink.jelly.cli.command

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class VersionSpec extends AnyWordSpec, Matchers:
  "version command" should {
    "print something" in {
      val (out, err) = Version.runTestCommand(List("version"))
      out should startWith("jelly-cli")
      out should include("Jelly-JVM")
      out should include("Apache Jena")
    }
  }
  "--version command" should {
    "print version" in {
      val (out, err) = Version.runTestCommand(List("--version"))
      out should startWith("jelly-cli")
      out should include("Jelly-JVM")
      out should include("Apache Jena")
    }
  }
