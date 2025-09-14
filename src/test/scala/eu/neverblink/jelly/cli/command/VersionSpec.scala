package eu.neverblink.jelly.cli.command

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class VersionSpec extends AnyWordSpec, Matchers:
  for alias <- Seq("version", "v", "--version") do
    s"$alias command" should {
      "print something" in {
        val (out, err) = Version.runTestCommand(List(alias))
        out should startWith("jelly-cli")
        out should include("Jelly-JVM")
        out should include("Apache Jena")
      }

      "report that reflection is supported" in {
        val (out, err) = Version.runTestCommand(List(alias))
        out should include("[X] JVM reflection: supported.")
      }

      "report that large XML parsing is supported by default if running under JVM <= 23" in {
        assume(Runtime.version().feature() <= 23, "Test only valid for JVM <= 23")
        val (out, err) = Version.runTestCommand(List(alias))
        out should include("[X] Large RDF/XML file parsing: supported.")
      }

      "report that large XML parsing is not supported by default if running under JVM >= 24" in {
        assume(Runtime.version().feature() >= 24, "Test only valid for JVM >= 24")
        val (out, err) = Version.runTestCommand(List(alias))
        out should include("[ ] Large RDF/XML file parsing: not supported.")
      }

      "include the copyright year" in {
        val (out, err) = Version.runTestCommand(List(alias))
        val currentYear = java.time.Year.now.getValue.toString
        out should include(s"Copyright (C) $currentYear NeverBlink and contributors")
      }

      "include a link to the license" in {
        val (out, err) = Version.runTestCommand(List(alias))
        out should include("https://www.apache.org/licenses/LICENSE-2.0")
      }
    }
