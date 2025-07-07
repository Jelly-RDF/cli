package eu.neverblink.jelly.cli.util.jena

import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.core.Quad
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.system.StreamRDFLib
import java.io.OutputStream
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr

/** A StreamRDF implementation that collects everything into a Model. When finishing, formats
  * everything according to the lang, and emits it to the outputStream. This is meant to be a
  * fallback for non-streaming RDF formats, as it requires all data to be loaded in memory.
  */
class StreamRdfBatchWriter(val outputStream: OutputStream, val lang: Lang) extends StreamRDF:
  private val model: Model = ModelFactory.createDefaultModel()
  private val modelStream: StreamRDF = StreamRDFLib.graph(model.getGraph)
  override def quad(quad: Quad): Unit = modelStream.quad(quad)
  override def triple(triple: Triple): Unit = modelStream.triple(triple)
  override def prefix(prefix: String, iri: String): Unit = modelStream.prefix(prefix, iri)
  override def base(base: String): Unit = modelStream.base(base)
  override def finish(): Unit = RDFDataMgr.write(outputStream, model, lang)
  override def start(): Unit = ()
