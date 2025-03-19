package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.RDFDataMgr

import scala.util.Using

/*
 * This class will be used to generate test data
 */
object DataGenHelper:

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

  /*
   * This method generates a Jelly stream with nTriples
   * @param nTriples number of triples to generate
   */
  def generateJelly(nTriple: Int): ByteArrayOutputStream =
    val model = generateTripleModel(nTriple)
    val newStream = ByteArrayOutputStream()
    // TODO: Add tests for different variants of Jelly (small strict etc)
    Using.resource(newStream) { stream =>
      RDFDataMgr.write(stream, model, JellyLanguage.JELLY)
    }
    newStream
