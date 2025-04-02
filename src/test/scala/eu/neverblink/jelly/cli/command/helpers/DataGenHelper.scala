package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFLanguages}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

/** This class will be used to generate test data
  */
object DataGenHelper:

  /** This method generates a triple model with nTriples
    * @param nTriples
    *   number of triples to generate
    * @return
    *   Model
    */
  def generateTripleModel(nTriples: Int): Model =
    val model = ModelFactory.createDefaultModel()
    val subStr = "http://example.org/subject/index"
    val predStr = "http://example.org/predicate/index"
    val objStr = "http://example.org/object/index"
    val tripleList = (1 to nTriples).map { i =>
      val sub = ResourceFactory.createResource(s"$subStr$i")
      val pred = ResourceFactory.createProperty(s"$predStr$i")
      val obj = ResourceFactory.createResource(s"$objStr$i")
      val stat = ResourceFactory.createStatement(sub, pred, obj)
      model.add(stat)
    }
    model

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

  /** Generate a Jelly frame in the Text Format.
    * @param nTriples
    *   number of triples to generate
    * @return
    *   String
    */
  def generateJellyText(nTriples: Int): String =
    val bytes = generateJellyBytes(nTriples)
    val frame = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(bytes))
    frame.get.toProtoString

  /** This method generates a Jelly byte input stream with nTriples
    * @param nTriples
    *   number of triples to generate
    */
  def generateJellyInputStream(nTriples: Int): ByteArrayInputStream =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, JellyLanguage.JELLY)
    val jellyStream = new ByteArrayInputStream(outputStream.toByteArray)
    jellyStream

  /** This method generates a NQuad string with nTriples
    * @param nTriples
    *   number of triples to generate
    * @return
    *   String
    */
  def generateJenaString(nTriples: Int, jenaLang: Lang = RDFLanguages.NQUADS): String =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, jenaLang)
    outputStream.toString

  /** This method generates an NQuad input stream with nTriples
    *
    * @param nTriples
    *   number of triples to generate
    * @return
    *   ByteArrayInputStream
    */
  def generateJenaInputStream(
      nTriples: Int,
      jenaLang: Lang = RDFLanguages.NQUADS,
  ): ByteArrayInputStream =
    val model = generateTripleModel(nTriples)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, model, jenaLang)
    val nQuadStream = new ByteArrayInputStream(outputStream.toByteArray)
    nQuadStream
