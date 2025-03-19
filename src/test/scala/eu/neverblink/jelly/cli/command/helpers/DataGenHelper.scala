package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.RDFDataMgr

import java.io.FileOutputStream
import java.nio.file.{Files, Paths}
import scala.util.Using

/*
 * This class will be used to generate test data
 */
object DataGenHelper:

  private val testFile = "testInput.jelly"

  /*
   * This method generates a triple model with nTriples
   * @param nTriples number of triples to generate
   * @return Model
   */
  def generateTripleModel(nTriples: Int): Model =
    val model = ModelFactory.createDefaultModel()
    val subStr = "http://example.org/subject"
    val predStr = "http://example.org/predicate"
    val objStr = "http://example.org/object"
    val tripleList = (1 to nTriples).map { i =>
      val sub = ResourceFactory.createResource(s"$subStr/$i")
      val pred = ResourceFactory.createProperty(s"$predStr/$i")
      val obj = ResourceFactory.createResource(s"$objStr/$i")
      val stat = ResourceFactory.createStatement(sub, pred, obj)
      model.add(stat)
    }
    model

  /* This method generates a Jelly file with nTriples
   * @param nTriples number of triples to generate
   * @param fileName name of the file to generate
   * @return String
   */
  def generateJellyFile(nTriples: Int): Unit =
    val model = generateTripleModel(nTriples)
    // TODO: Add configurable generation for different variants of Jelly (small strict etc)
    Using.resource(FileOutputStream(testFile)) { file =>
      RDFDataMgr.write(file, model, JellyLanguage.JELLY)
    }

  /* This method cleans up the file after the test*/
  def cleanUpFile(): Unit =
    Files.deleteIfExists(Paths.get(testFile))
