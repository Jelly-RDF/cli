package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages, Lang}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer
import scala.util.Using

/** This class will be used to generate test data
  */
object DataGenHelper:
  private val testDir = "test"
  private val testFile = "testInput.jelly"
  private val inputStream = System.in
  protected val outputFiles = ListBuffer[String]()

  /** This method generates a triple model with nTriples
    * @param nTriples
    *   number of triples to generate
    * @return
    *   Model
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

  /** This method generates a Jelly file with nTriples
    * @param nTriples
    *   number of triples to generate
    * @return
    *   String
    */
  def generateJellyFile(nTriples: Int): String =
    val model = generateTripleModel(nTriples)
    // TODO: Add configurable generation for different variants of Jelly (small strict etc)
    Using.resource(FileOutputStream(testFile)) { file =>
      RDFDataMgr.write(file, model, JellyLanguage.JELLY)
    }
    testFile

  /** This method generates a Jelly output string
    *
    * @param nTriples
    *   number of triples to generate
    * @return
    *   String
    */
  def generateJellyString(nTriples: Int): String =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, JellyLanguage.JELLY)
    outputStream.toString

  /** This method generates a NQuad file with nTriples
    * @param nTriples
    *   number of triples to generate
    * @return
    *   String
    */
  def generateNQuadFile(nTriples: Int): String =
    val model = generateTripleModel(nTriples)
    Using.resource(FileOutputStream(testFile)) { file =>
      RDFDataMgr.write(file, model, RDFLanguages.NQUADS)
    }
    testFile

  /** This method generates a Jelly byte input stream with nTriples
    * @param nTriples
    *   number of triples to generate
    */
  def generateJellyInputStream(nTriples: Int): Unit =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, JellyLanguage.JELLY)
    val jellyStream = new ByteArrayInputStream(outputStream.toByteArray)
    System.setIn(jellyStream)

  /** This method generates a NQuad string with nTriples
    * @param nTriples
    *   number of triples to generate
    * @return
    *   String
    */
  def generateNQuadString(nTriples: Int): String =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, RDFLanguages.NQUADS)
    outputStream.toString

  /** This method generates an NQuad input stream with nTriples
    *
    * @param nTriples
    *   number of triples to generate
    * @return
    *   String
    */
  def generateNQuadInputStream(nTriples: Int): Unit =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, RDFLanguages.NQUADS)
    val nQuadStream = new ByteArrayInputStream(outputStream.toByteArray)
    System.setIn(nQuadStream)

  /** Make test dir
    */
  def makeTestDir(): String =
    Files.createDirectories(Paths.get(testDir))
    testDir

  /** Generates and then cleans the file for test purposes
    */
  def generateOutputFile(format: Lang = RDFLanguages.NQUADS): String =
    if !Files.exists(Paths.get(testDir)) then makeTestDir()
    val extension = format.getFileExtensions.get(0)
    val fileName = s"${testDir}/testOutput${outputFiles.size}.${extension}"
    outputFiles += fileName
    fileName

  def cleanUpFiles(): Unit =
    Files.deleteIfExists(Paths.get(testFile))
    for file <- outputFiles do Files.deleteIfExists(Paths.get(file))
    Files.deleteIfExists(Paths.get(testDir))

  def resetInputStream(): Unit =
    System.setIn(inputStream)
