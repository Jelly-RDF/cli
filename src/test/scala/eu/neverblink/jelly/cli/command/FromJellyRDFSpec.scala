package eu.neverblink.jelly.cli.command

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FromJellyRDFSpec extends AnyWordSpec, Matchers:
  "from-jelly-rdf command" should {
    "print something" in {
      val (out, err) = FromJellyRDF.runTest(FromJellyRDFOptions())
      out should startWith("jelly-cli")
      out should include("Jelly-JVM")
      out should include("Apache Jena")
    }
  }
