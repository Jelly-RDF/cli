package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import scala.util.Using

/*
 * This class will be used to generate test data
 */
object DataGenHelper:

  private val testFile = "testInput.jelly"
  private val inputStream = System.in

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
  def generateJellyFile(nTriples: Int): String =
    val model = generateTripleModel(nTriples)
    // TODO: Add configurable generation for different variants of Jelly (small strict etc)
    Using.resource(FileOutputStream(testFile)) { file =>
      RDFDataMgr.write(file, model, JellyLanguage.JELLY)
    }
    testFile

  /*
   * This method generates a Jelly byte input stream with nTriples
   * @param nTriples number of triples to generate
   * @return String
   */
  def generateJellyInputStream(nTriples: Int): ByteArrayInputStream =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, JellyLanguage.JELLY)
    new ByteArrayInputStream(outputStream.toByteArray)

  /*
   * This method generates a NQuad string with nTriples
   * @param nTriples number of triples to generate
   * @return String
   */
  def generateNQuadString(nTriples: Int): String =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, RDFLanguages.NQUADS)
    outputStream.toString

  def cleanUpFile(): Unit =
    Files.deleteIfExists(Paths.get(testFile))

  def resetInputStream(): Unit =
    System.setIn(inputStream)
