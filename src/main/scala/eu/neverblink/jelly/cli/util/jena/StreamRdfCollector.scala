package eu.neverblink.jelly.cli.util.jena

import eu.neverblink.jelly.core.NamespaceDeclaration
import org.apache.jena.graph.Triple
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.sparql.core.Quad

import scala.collection.mutable

/** A StreamRDF implementation that collects everything incoming into a single collection. This is
  * not meant to be very scalable or performant.
  */
final class StreamRdfCollector extends StreamRDF:
  private val buffer = mutable.ArrayBuffer.empty[RdfElement]

  def getBuffer: Seq[RdfElement] = buffer.toSeq

  def replay(to: StreamRDF): Unit =
    getBuffer.foreach {
      case t: Triple => to.triple(t)
      case q: Quad => to.quad(q)
      case ns: NamespaceDeclaration => to.prefix(ns.prefix, ns.iri)
    }

  override def start(): Unit = ()

  override def triple(triple: Triple): Unit =
    buffer += triple

  override def quad(quad: Quad): Unit =
    buffer += quad

  override def base(base: String): Unit = ()

  override def prefix(prefix: String, iri: String): Unit =
    buffer += NamespaceDeclaration(prefix, iri)

  override def finish(): Unit = ()
