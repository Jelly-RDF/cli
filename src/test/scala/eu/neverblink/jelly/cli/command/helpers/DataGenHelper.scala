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
class DataGenHelper(testDir: String = "test"):
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
    val newFile = generateFile(JellyLanguage.JELLY)
    // TODO: Add configurable generation for different variants of Jelly (small strict etc)
    Using.resource(FileOutputStream(newFile)) { file =>
      RDFDataMgr.write(file, model, JellyLanguage.JELLY)
    }
    newFile

  /** This method generates a Jelly byte array
    *
    * @param nTriples
    *   number of triples to generate
    * @return
    *   String
    */
  def generateJellyBytes(nTriples: Int): Array[Byte] =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, JellyLanguage.JELLY)
    outputStream.toByteArray

  /** This method generates a NQuad file with nTriples
    * @param nTriples
    *   number of triples to generate
    * @return
    *   String
    */
  def generateNQuadFile(nTriples: Int): String =
    val model = generateTripleModel(nTriples)
    val newFile = generateFile(RDFLanguages.NQUADS)
    Using.resource(FileOutputStream(newFile)) { file =>
      RDFDataMgr.write(file, model, RDFLanguages.NQUADS)
    }
    newFile

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

  /** Generates the file for test purposes
    */
  def generateFile(format: Lang = RDFLanguages.NQUADS): String =
    if !Files.exists(Paths.get(testDir)) then makeTestDir()
    val extension = format.getFileExtensions.get(0)
    val fileName = s"${testDir}/testOutput${outputFiles.size}.${extension}"
    outputFiles += fileName
    fileName

  def cleanUpFiles(): Unit =
    for file <- outputFiles do Files.deleteIfExists(Paths.get(file))
    Files.deleteIfExists(Paths.get(testDir))

  def resetInputStream(): Unit =
    System.setIn(inputStream)
