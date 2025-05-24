package eu.neverblink.jelly.cli.command.helpers

import eu.neverblink.jelly.convert.jena.riot.JellyLanguage
import eu.neverblink.jelly.core.proto.google.v1 as google
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFLanguages}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

/** This class will be used to generate test data
  */
object DataGenHelper:

  /** This method generates a triple model with nTriples
    * @param nTriples
    *   number of triples to generate
    * @param differentiator
    *   string to include in iris, to make sure the generated models are different
    * @return
    *   Model
    */
  def generateTripleModel(nTriples: Int, differentiator: String = ""): Model =
    val model = ModelFactory.createDefaultModel()
    val subStr = f"http://example.org/subject/index$differentiator"
    val predStr = f"http://example.org/predicate/index$differentiator"
    val objStr = f"http://example.org/object/index$differentiator"
    val tripleList = (1 to nTriples).map { i =>
      val sub = ResourceFactory.createResource(s"$subStr$i")
      val pred = ResourceFactory.createProperty(s"$predStr$i")
      val obj = ResourceFactory.createResource(s"$objStr$i")
      val stat = ResourceFactory.createStatement(sub, pred, obj)
      model.add(stat)
    }
    model

  /** This method generates an RDF dataset with nGraphs and nTriplesPerGraph
    * @param nGraphs
    *   number of named graphs to generate
    * @param nTriplesPerGraph
    *   number of triples per graph to generate
    * @param differentiator
    *   string to include in iris, to make sure the generated datasets are different
    * @return
    *   Dataset
    */
  def generateDataset(nGraphs: Int, nTriplesPerGraph: Int, differentiator: String): Dataset =
    val dataset =
      DatasetFactory.create(generateTripleModel(nTriplesPerGraph, f"/${differentiator}default_"))
    for i <- 1 to nGraphs do
      val model = generateTripleModel(nTriplesPerGraph, f"/${i}_")
      val graphName =
        ResourceFactory.createResource(s"http://example.org/graph/$differentiator$i")
      dataset.addNamedModel(graphName, model)
    dataset

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
    val frame = google.RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(bytes))
    frame.toString

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
    * @param jenaLang
    *   the language to use for the output
    * @return
    *   String
    */
  def generateJenaString(
      nTriples: Int,
      jenaLang: Lang = RDFLanguages.NQUADS,
      dataset: Boolean = false,
  ): String =
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

  /** This method generates a serialized RDF dataset input stream
    *
    * @param nGraphs
    *   number of named graphs to generate
    * @param nTriples
    *   number of triples to generate per graph
    * @param jenaLang
    *   the language to use for the output
    * @param differentiator
    *   string to include in iris, to make sure the generated datasets are different
    * @return
    *   ByteArrayInputStream
    */
  def generateJenaInputStreamDataset(
      nGraphs: Int,
      nTriples: Int,
      jenaLang: Lang = RDFLanguages.NQUADS,
      differentiator: String = "",
  ): ByteArrayInputStream =
    val dataset = generateDataset(nGraphs, nTriples, differentiator)
    val outputStream = new ByteArrayOutputStream()
    RDFDataMgr.write(outputStream, dataset, jenaLang)
    val nQuadStream = new ByteArrayInputStream(outputStream.toByteArray)
    nQuadStream
