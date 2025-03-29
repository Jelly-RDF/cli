package eu.neverblink.jelly.cli.command

import eu.neverblink.jelly.cli.{ExitException, InvalidFormatSpecified}
import eu.neverblink.jelly.cli.command.helpers.{DataGenHelper, TestFixtureHelper}
import eu.neverblink.jelly.cli.command.rdf.{RdfFormatOption, RdfToJelly, RdfToJellyPrint}
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.apache.jena.riot.RDFParser

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
    "handle conversion of NTriples to Jelly" when {
      "a file to output stream" in withFullQuadFile { f =>
        val (out, err) =
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
      }

      "a file to file" in withFullQuadFile { f =>
        withEmptyJellyFile { j =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--to", j))
          val content = translateJellyBack(new FileInputStream(j))
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        }
      }
      "input stream to output stream" in {
        val input = DataGenHelper.generateNQuadInputStream(testCardinality)
        RdfToJelly.setStdIn(input)
        val tripleModel = DataGenHelper.generateTripleModel(testCardinality)
        val (out, err) = RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format", RdfFormatOption.NQuads.cliOptions.head),
        )
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(tripleModel.listStatements())
      }
      "an input stream to file" in withEmptyJellyFile { j =>
        val input = DataGenHelper.generateNQuadInputStream(testCardinality)
        RdfToJelly.setStdIn(input)
        val tripleModel = DataGenHelper.generateTripleModel(testCardinality)
        val (out, err) = RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--to", j))
        val content = translateJellyBack(new FileInputStream(j))
        content.containsAll(tripleModel.listStatements())
      }
    }
  }
  "throw proper exception" when {
    "invalid format is specified" in withFullQuadFile { f =>
      val exception =
        intercept[ExitException] {
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--in-format", "invalid"))
        }
      val msg = InvalidFormatSpecified("invalid", RdfToJellyPrint.validFormatsString)
      RdfToJelly.getErrString should include(msg.getMessage)
      exception.code should be(1)
    }

  }
