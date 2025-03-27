package eu.neverblink.jelly.cli.command

import eu.neverblink.jelly.cli.{ExitException, InvalidFormatSpecified}
import eu.neverblink.jelly.cli.command.helpers.{CleanUpAfterTest, DataGenHelper}
import eu.neverblink.jelly.cli.command.rdf.{RdfFormatOption, RdfToJelly, RdfToJellyPrint}
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.apache.jena.riot.RDFParser

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import scala.util.Using

class RdfToJellySpec extends AnyWordSpec with Matchers with CleanUpAfterTest:

  protected val dHelper: DataGenHelper = DataGenHelper("testRdfToJelly")

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
      "a file to output stream" in {
        val nQuadFile = dHelper.generateNQuadFile(3)
        val tripleModel = dHelper.generateTripleModel(3)
        val (out, err) =
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", nQuadFile))
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(tripleModel.listStatements())
      }

      "a file to file" in {
        val nQuadFile = dHelper.generateNQuadFile(3)
        val newFile = dHelper.generateFile(JellyLanguage.JELLY)
        val tripleModel = dHelper.generateTripleModel(3)
        val (out, err) =
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", nQuadFile, "--to", newFile))
        val content = translateJellyBack(new FileInputStream(newFile))
        content.containsAll(tripleModel.listStatements())
      }

      "input stream to output stream" in {
        val testNumber = 10
        val input = dHelper.generateNQuadInputStream(testNumber)
        RdfToJelly.setStdIN(input)
        val tripleModel = dHelper.generateTripleModel(testNumber)
        val (out, err) = RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format", RdfFormatOption.NQuads.cliOptions.head),
        )
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(tripleModel.listStatements())
      }

      "an input stream to file" in {
        val testNumber = 23
        val input = dHelper.generateNQuadInputStream(testNumber)
        RdfToJelly.setStdIN(input)
        val newFile = dHelper.generateFile(JellyLanguage.JELLY)
        val tripleModel = dHelper.generateTripleModel(testNumber)
        val (out, err) = RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--to", newFile))
        val content = translateJellyBack(new FileInputStream(newFile))
        content.containsAll(tripleModel.listStatements())
      }
    }
    "throw proper exception" when {
      "invalid format is specified" in {
        val jellyFile = dHelper.generateNQuadFile(3)
        val exception =
          intercept[ExitException] {
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", jellyFile, "--in-format", "invalid"))
          }
        val msg = InvalidFormatSpecified("invalid", RdfToJellyPrint.validFormatsString)
        RdfToJelly.getErrString should include(msg.getMessage)
        exception.code should be(1)
      }
    }
  }
