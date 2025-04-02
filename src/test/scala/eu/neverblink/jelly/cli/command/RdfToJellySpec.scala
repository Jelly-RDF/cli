package eu.neverblink.jelly.cli.command

import eu.neverblink.jelly.cli.{ExitException, InvalidFormatSpecified}
import eu.neverblink.jelly.cli.command.helpers.{DataGenHelper, TestFixtureHelper}
import eu.neverblink.jelly.cli.command.rdf.{RdfFormat, RdfToJelly, RdfToJellyPrint}
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.apache.jena.riot.{RDFLanguages, RDFParser}

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import scala.util.Using

class RdfToJellySpec extends AnyWordSpec with TestFixtureHelper with Matchers:

  protected val testCardinality: Integer = 33

  def translateJellyBack(inputStream: InputStream): Model =
    Using(inputStream) { content =>
      val newModel = ModelFactory.createDefaultModel()
      RDFParser.source(content).lang(JellyLanguage.JELLY).parse(newModel)
      newModel
    } match {
      case scala.util.Success(value) => value
      case scala.util.Failure(exception) => throw exception
    }

  "rdf to-jelly command" should {
    "handle conversion of NQuads to Jelly" when {
      "a file to output stream" in withFullJenaFile { f =>
        val (out, err) =
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
      }

      "a file to file" in withFullJenaFile { f =>
        withEmptyJellyFile { j =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--to", j))
          val content = translateJellyBack(new FileInputStream(j))
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        }
      }
      "input stream to output stream" in {
        val input = DataGenHelper.generateJenaInputStream(testCardinality)
        RdfToJelly.setStdIn(input)
        val tripleModel = DataGenHelper.generateTripleModel(testCardinality)
        val (out, err) = RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format", RdfFormat.NQuads.cliOptions.head),
        )
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(tripleModel.listStatements())
      }
      "an input stream to file" in withEmptyJellyFile { j =>
        val input = DataGenHelper.generateJenaInputStream(testCardinality)
        RdfToJelly.setStdIn(input)
        val tripleModel = DataGenHelper.generateTripleModel(testCardinality)
        val (out, err) = RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--to", j))
        val content = translateJellyBack(new FileInputStream(j))
        content.containsAll(tripleModel.listStatements())
      }
    }
    "handle conversion of other formats to Jelly" when {
      "NTriples" in {
        val input = DataGenHelper.generateJenaInputStream(testCardinality, RDFLanguages.NTRIPLES)
        RdfToJelly.setStdIn(input)
        val (out, err) =
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--in-format", "nt"))
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
      }
      "Turtle" in {
        val input = DataGenHelper.generateJenaInputStream(testCardinality, RDFLanguages.TURTLE)
        RdfToJelly.setStdIn(input)
        val (out, err) =
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--in-format", "ttl"))
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
      }
      "TriG" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        },
        jenaLang = RDFLanguages.TRIG,
      )
      "RDF Protobuf" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--in-format", "rdfp"))
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        },
        jenaLang = RDFLanguages.RDFPROTO,
      )
      "RDF Thrift" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        },
        jenaLang = RDFLanguages.RDFTHRIFT,
      )
      "RDF/XML" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--in-format", "rdfxml"))
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        },
        jenaLang = RDFLanguages.RDFXML,
      )
    }
    "throw proper exception" when {
      "invalid format is specified" in withFullJenaFile { f =>
        val exception =
          intercept[ExitException] {
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--in-format", "invalid"))
          }
        val msg = InvalidFormatSpecified("invalid", RdfToJellyPrint.validFormatsString)
        RdfToJelly.getErrString should include(msg.getMessage)
        exception.code should be(1)
      }
      "invalid format out of existing is specified" in withFullJenaFile { f =>
        val exception =
          intercept[ExitException] {
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--in-format", "jelly-text"))
          }
        val msg = InvalidFormatSpecified("jelly-text", RdfToJellyPrint.validFormatsString)
        RdfToJelly.getErrString should include(msg.getMessage)
        exception.code should be(1)
      }

    }
  }
