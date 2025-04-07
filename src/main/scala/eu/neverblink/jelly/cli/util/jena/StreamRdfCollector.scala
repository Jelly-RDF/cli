package eu.neverblink.jelly.cli.util.jena

import eu.ostrzyciel.jelly.core.NamespaceDeclaration
import org.apache.jena.graph.Triple
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.sparql.core.Quad

/** A StreamRDF implementation that collects everything incoming into a single collection. This is
  * not meant to be very scalable or performant.
  */
final class StreamRdfCollector extends StreamRDF:
  private val buffer = scala.collection.mutable.ArrayBuffer.empty[RdfElement]
  private var _hasTriples = false
  private var _hasQuads = false
  private var _hasNamespaceDeclarations = false

  def getBuffer: Seq[RdfElement] = buffer.toSeq

  def hasTriples: Boolean = _hasTriples
  def hasQuads: Boolean = _hasQuads
  def hasNamespaceDeclarations: Boolean = _hasNamespaceDeclarations

  def replay(to: StreamRDF): Unit =
    getBuffer.foreach {
      case t: Triple => to.triple(t)
      case q: Quad => to.quad(q)
      case ns: NamespaceDeclaration => to.prefix(ns.prefix, ns.iri)
    }

  override def start(): Unit = ()

  override def triple(triple: Triple): Unit =
    _hasTriples = true
    buffer += triple

  override def quad(quad: Quad): Unit =
    _hasQuads = true
    buffer += quad

  override def base(base: String): Unit = ()

  override def prefix(prefix: String, iri: String): Unit =
    _hasNamespaceDeclarations = true
    buffer += NamespaceDeclaration(prefix, iri)

  override def finish(): Unit = ()
