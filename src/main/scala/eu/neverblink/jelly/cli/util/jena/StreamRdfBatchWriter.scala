package eu.neverblink.jelly.cli.util.jena

import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.sparql.core.Quad
import org.apache.jena.graph.Triple
import org.apache.jena.riot.system.StreamRDFLib
import java.io.OutputStream
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.query.DatasetFactory
import org.apache.jena.query.Dataset

/** A StreamRDF implementation that collects everything into a Model. When finishing, formats
  * everything according to the lang, and emits it to the outputStream. This is meant to be a
  * fallback for non-streaming RDF formats, as it requires all data to be loaded in memory.
  */
class StreamRdfBatchWriter(val outputStream: OutputStream, val lang: Lang) extends StreamRDF:
  protected val dataset: Dataset = DatasetFactory.create()
  protected val datasetStream: StreamRDF = StreamRDFLib.dataset(dataset.asDatasetGraph())
  override def quad(quad: Quad): Unit = datasetStream.quad(quad)
  override def triple(triple: Triple): Unit = datasetStream.triple(triple)
  override def prefix(prefix: String, iri: String): Unit = datasetStream.prefix(prefix, iri)
  override def base(base: String): Unit = datasetStream.base(base)
  override def finish(): Unit = writeOutput()
  override def start(): Unit = ()
  def writeOutput(): Unit =
    if lang == Lang.RDFXML then
      RDFDataMgr.write(outputStream, dataset.getDefaultModel, lang)
    else
      RDFDataMgr.write(outputStream, dataset, lang)
