package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFLanguages}

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  FileOutputStream,
  InputStream,
  PrintStream,
}
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer
import scala.util.Using

/** This class will be used to generate test data
  */
class DataGenHelper(testDir: String = "test"):
  protected val outputFiles = ListBuffer[String]()
  protected val inputStream = new ThreadLocal[InputStream]()
  private val outputStream = new ThreadLocal[ByteArrayOutputStream]()
  private val printStream = new ThreadLocal[PrintStream]()

  /** Sets a thread safe input stream with supplied data
    * @param data
    */
  def setInputStream(data: Array[Byte]): Unit = {
    inputStream.set(new ByteArrayInputStream(data))
    System.setIn(inputStream.get())
  }

  /** Resets streams after every tests
    */
  def resetStreams(): Unit = {
    System.setIn(System.in)
    System.setOut(System.out)
    inputStream.remove()
    outputStream.remove()
    printStream.remove()
  }

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
    this.setInputStream(outputStream.toByteArray)

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
    this.setInputStream(outputStream.toByteArray)

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
