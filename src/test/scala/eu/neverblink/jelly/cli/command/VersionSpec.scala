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
